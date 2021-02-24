import copy
import json
import os
import time
from datetime import datetime
from uuid import uuid4
from decimal import Decimal

from chalice import Response
from n_utils.utils import query_items_by_id, replace_dict_key, logger, authenticate, error_response, parse_decimal, \
    batch_put_item, batch_delete_item, mf_table_name_to_thread_var, update_inactive_entity_status, update_db_record, \
    db_update_item_helper, get_moneyflow_table, get_gen_table, log_exception, query_items_by_id_list_v2, to_dec, \
    assert_company_pending_event, add_pending_status, delete_pending_status, get_db_item, get_debug_environment, \
    get_audit_rec
from n_utils.keys_structure import subtotal_entity_balance_record_pk, owners_pk as owner_pk, \
    payments_pk, payments_sk, creditors_pk as creditor_pk, subtotal_entity_balance_record_sk, \
    processed_disbursement_pk as processed_disb_pk, processed_disbursement_sk as processed_disb_sk
from n_utils.boto_clients import lambda_client
from n_utils.ses_wrapper import send_notification_to_email_via_ses

from chalicelib.delegate_companies import get_company_email, get_company_details, check_mandatory_company_entity_ids
from chalicelib.delegate_companies_class import Companies
from chalicelib.delegate_logger import log_sns_disb_problem
from chalicelib.delegate_users import get_user_email
from chalicelib.moneyflow.delegate_pending_payments import pending_payment_trans_type_to_ledger_from_entity_type
from chalicelib.moneyflow.delegate_report import get_comma_separated_list
from chalicelib.moneyflow.delegate_statement_report import get_transactions_from_last_disbursement_by_id
from chalicelib.moneyflow.delegate_transactions_class import Transactions
from chalicelib.moneyflow.disbursement.delegate_disbursement import get_disb_payments, get_disbursement_record
from chalicelib.moneyflow.disbursement.disburse_owners import add_gst
from chalicelib.moneyflow.disbursement.disbursement_notification import get_disb_summary_text
from chalicelib.moneyflow.disbursement.reporting import DisbursementReporting
from chalicelib.moneyflow.disbursement.enough_entity_balance_checker import check_balances_for_disburse


def get_bank_transactions_amount(transactions):
    amount = Decimal(0)
    for transaction in transactions:
        if transaction['transaction_type'].lower() == 'bank':
            amount += transaction.get('amount', 0)
    return amount


class DisbursementFinalization:

    def __init__(self, msg: dict):
        logger.debug(f'DisbursementFinalization init message: {msg}')
        self.disb_id = str(uuid4())
        self.disb_datetime = datetime.utcnow().isoformat(timespec='seconds')
        self.rollback = {
            'entities_before_disb': {
                'owners': [],
                'creditors': []
            },
            'deactivated_payments': [],
            'disb_subtotals': [],
            'created_transactions': [],
            'used_transactions': [],
            'disbursement_id': self.disb_id
        }
        self.message = msg
        self.user_id = msg.get('user_id', None)
        self.company_id = msg.get('company_id', None)
        self.disbursement = get_disbursement_record(self.company_id, self.user_id)
        self.email = get_user_email(self.user_id)
        self.company_email = get_company_email(self.company_id)
        self.py_lambda_email_list = [self.email, self.company_email]
        self.java_lambda_email_list = get_comma_separated_list(self.py_lambda_email_list)
        self.owners_to_disburse = self.disbursement.get('owners_to_disburse', [])
        self.creditors_to_disburse = self.disbursement.get('creditors_to_disburse', [])
        self.owners_ids = [owner['id'] for owner in self.owners_to_disburse]
        self.creditors_ids = [creditor['creditor_id'] for creditor in self.creditors_to_disburse]
        self.owners_records = []
        self.creditors_records = []
        self.pending_payments = self.disbursement.get('payments', [])
        self.pending_payments_db_records = []
        self.process_name = "disbursement_finalization"

    @staticmethod
    def lambda_disbursement_finalization(event: dict, context: dict):
        logger.debug(f'DisbursementFinalization::lambda_disbursement_finalization: event:{event}, context: {context}')
        company_id = None
        try:
            company_id = event.get('company_id', None)
            mf_table_name_to_thread_var(company_id)
            DisbursementFinalization(event).run()
        except Exception as e:
            logger.exception(
                f'DisbursementFinalization::lambda_disbursement_finalization: event:{event}, context: {context}')

            log_sns_disb_problem(
                sub='Disbursement finalization',
                msg=f'lambda_disbursement_finalization error: {e}. See logs',
                company_id=company_id
            )

    def run(self):
        try:
            try:
                add_pending_status(self.company_id, self.process_name)
                self.create_disb_summary_record()
                self.get_disbursement_entities_records()
                self.del_rent_trans_from_disb_subtotal()
                self.pending_payments_db_records = query_items_by_id_list_v2(
                    payments_pk.format(company_id=self.company_id),
                    [payment['id'] for payment in self.pending_payments],
                    table=get_moneyflow_table
                )
                entities_balances_after_last_disb = self.get_entities_balances_after_last_disb()
                entities_to_disburse_ids = self.get_entities_to_disburse_ids()
                self.update_disb_summary_record(self.company_id, self.disb_id, {
                    'pending_payment_records_ids': [payment['id_'] for payment in self.pending_payments_db_records],
                    'entities_balances_after_last_disb': entities_balances_after_last_disb,
                    'entities_to_disburse_ids': entities_to_disburse_ids,
                    'status': 'in progress'
                })
                self.set_disb_to_transactions()
                journal_trans_to_db, eft_trans_to_db, bpay_trans_to_db, payment_trans_to_db = self.get_disb_transactions_to_create()
                self.update_disb_summary_record(self.company_id, self.disb_id, {
                    'bpay_transaction_audits': [trans['audit'] for trans in bpay_trans_to_db],
                    'eft_transaction_audits': [trans['audit'] for trans in eft_trans_to_db]
                })
                all_transactions = [*journal_trans_to_db, *eft_trans_to_db, *bpay_trans_to_db, *payment_trans_to_db]
                self.save_balance_after_last_disbursement(all_transactions)
                self.put_transactions_to_db(all_transactions)
                self.deactivate_payments()
                self.update_disb_summary_record(self.company_id, self.disb_id, {
                    'status': 'processed'
                })
            finally:
                delete_pending_status(self.company_id, self.process_name)
            self.send_notification(bpay_trans_to_db, eft_trans_to_db, journal_trans_to_db, payment_trans_to_db)
            if get_debug_environment():
                DisbursementReporting(self.company_id, self.disb_id).do_reporting()
            else:
                message = {"company_id": self.company_id, "disbursement_id": self.disb_id}
                response = lambda_client.invoke(
                    FunctionName=os.environ["LAMBDA_DISBURSEMENT_REPORTING"],
                    InvocationType="Event",
                    Payload=json.dumps(message)
                )
                logger.info(f'DisbursementFinalization:: run: LAMBDA_DISBURSEMENT_REPORTING INVOKED, '
                            f'response: {response}')
        except Exception as error:
            log_exception(error)
            self.do_rollback()

    def create_disb_summary_record(self):
        record = {
            'partkey': processed_disb_pk.format(company_id=self.company_id),
            'sortkey': processed_disb_sk.format(disbursement_id=self.disb_id),
            'record_type': 'processed_disbursement',
            'pending_payment_records_ids': [payment['id_'] for payment in self.pending_payments_db_records],
            'id_': self.disb_id,
            'disbursement_datetime': self.disb_datetime,
            'company_id': self.company_id,
            'user_id': self.user_id,
            'requested_trust_account_reports': self.disbursement.get('trust_account_reports', []),
            'status': 'initiated',
            'audit_partition': get_audit_rec(self.company_id)['partition']
        }
        get_moneyflow_table().put_item(Item=record)

    @staticmethod
    def update_disb_summary_record(company_id, disb_id, update_dict):
        update_db_record({
            'partkey': processed_disb_pk.format(company_id=company_id),
            'sortkey': processed_disb_sk.format(disbursement_id=disb_id),
            **update_dict
        }, update_dict.keys(), table=get_moneyflow_table)

    def send_notification(self, bpay_trans_to_db, eft_trans_to_db, journal_trans_to_db,
                          payment_trans_to_db):
        # disb_datetime is in ISO format
        summary_name = f'Disbursement Summary {self.disb_datetime.replace("T", " ")}'
        disb_summary_info = {
            'journal_trans': journal_trans_to_db,
            'eft_trans': eft_trans_to_db,
            'bpay_trans': bpay_trans_to_db,
            'payment_trans': payment_trans_to_db,
            'owners_to_disburse': self.owners_to_disburse,
            'creditors_to_disburse': self.creditors_to_disburse,
            'payments': get_disb_payments(self.disbursement, self.company_id),
            'trust_account_reports': self.disbursement.get('trust_account_reports', []),
            'report_name': summary_name,
            'company_name': Companies.get_company_name(self.company_id)
        }
        send_notification_to_email_via_ses(
            self.py_lambda_email_list, f'{summary_name} from n_utils',
            get_disb_summary_text(self.user_id, self.company_id, disb_summary_info)
        )

    def get_entities_to_disburse_ids(self):
        logger.debug(f'DisbursementFinalization::get_entities_to_disburse_ids: self.__dict__:{self.__dict__}')
        return {
            'eft_owners_ids': [owner['id'] for owner in self.owners_to_disburse],
            'eft_creditors_ids': [creditor['creditor_id'] for creditor in self.creditors_to_disburse],
            'bpay_creditor_payment_ids': [
                {
                    'creditor_id': payment['creditor_id'],
                    'payment_id': payment['id']
                }
                for payment in self.pending_payments if payment.get('type') == 'BPAY'
            ]
        }

    def get_disbursement_entities_records(self):
        self.owners_records = query_items_by_id_list_v2(
            owner_pk.format(company_id=self.company_id),
            self.owners_ids,
            table=get_gen_table
        )
        self.creditors_records = query_items_by_id_list_v2(
            creditor_pk.format(company_id=self.company_id),
            self.creditors_ids,
            table=get_moneyflow_table
        )

    def set_disb_to_transactions(self):
        logger.debug(
            f'DisbursementFinalization::set_disb_to_transactions:  disb_datetime:{self.disb_datetime}, '
            f'disb_id:{self.disb_id}, self.__dict__:{self.__dict__}'
        )
        transactions = []
        entities = [*self.owners_records, *self.creditors_records]
        for entity in entities:
            transactions.extend(get_transactions_from_last_disbursement_by_id(entity))
        # remove possible duplicates
        transactions = [tr for n, tr in enumerate(transactions) if tr not in transactions[n + 1:]]
        self.rollback['used_transactions'].extend(copy.deepcopy(transactions))
        for tr in transactions:
            tr = self.add_disbursement_attributes(tr)
            get_moneyflow_table().update_item(**db_update_item_helper(['disbursement_datetime', 'disbursement_id'], tr))
        logger.info(f'DisbursementFinalization::set_disb_to_transactions: SUCCESS, disb_id:{self.disb_id}')

    def add_disbursement_attributes(self, transaction):
        transaction.update({
            'disbursement_datetime': self.disb_datetime,
            'disbursement_id': self.disb_id
        })
        return transaction

    def save_balance_after_last_disbursement(self, all_transactions):
        # Getting entities current balance  partkey: str, id_list: list, table=get_gen_table
        entities_ids = [*self.creditors_ids, *self.owners_ids]
        subtotal_entity_records = query_items_by_id_list_v2(
            subtotal_entity_balance_record_pk.format(company_id=self.company_id),
            entities_ids,
            table=get_moneyflow_table
        )
        subtotal_ids = [disb_subtotal["id_"] for disb_subtotal in subtotal_entity_records]
        for entity_id in set(entities_ids) - set(subtotal_ids):
            if entity_id in self.owners_ids:
                entity_type = "owner"
            else:
                entity_type = "creditor"
            subtotal_entity_records.append(
                DisbursementFinalization.get_clean_disb_subtotal(self.company_id, entity_id, entity_type))
        self.backup_changes_after_prev_disbursement()
        audit = '0'
        for tr in all_transactions:
            if Decimal(tr['audit']) > Decimal(audit):
                audit = tr['audit']
            else:
                continue
        for record in subtotal_entity_records:
            amount = record['current_balance']
            entity_id = record['id_']
            # getting transferred amounts for entity
            for tr in all_transactions:
                if tr.get('ledger_to', None) == entity_id and tr['ledger_type'] in ['Journal Credit', 'Receipt']:
                    amount += tr.get('amount', 0)
                elif tr.get('ledger_from', None) == entity_id and tr['ledger_type'] in ['Journal Debit', 'Payment']:
                    amount -= tr.get('amount', 0)
                else:
                    continue

            self.set_entity_last_disbursement(record["entity_type"], entity_id, amount, audit)

    def backup_changes_after_prev_disbursement(self):
        self.rollback["entities_before_disb"] = {"owners": self.owners_records,
                                                 "creditors": self.creditors_records}

    def set_entity_last_disbursement(self, entity_type, entity_id, amount, trans_audit):
        get_moneyflow_table().update_item(
            Key={
                'partkey': f'{entity_type}s_{self.company_id}',
                'sortkey': entity_id
            },
            UpdateExpression="set #last_payout_audit = :last_payout_audit, "
                             "#last_payout_date = :last_payout_date, "
                             "#balance_after_last_disbursement = :amount",
            ExpressionAttributeValues={':last_payout_audit': trans_audit,
                                       ':last_payout_date': datetime.now().date().isoformat(),
                                       ":amount": amount},
            ExpressionAttributeNames={'#last_payout_audit': 'last_payout_audit',
                                      '#last_payout_date': 'last_payout_date',
                                      '#balance_after_last_disbursement': 'balance_after_last_disbursement'}
        )
        logger.info(f'DisbursementFinalization::set_entity_last_disbursement: SUCCESS,'
                    f' entity_type: {entity_type}, entity_id: {entity_id}')

    def get_entities_balances_after_last_disb(self):
        owners = self.get_items_for_disbursement('owners', self.owners_ids, table=get_gen_table)
        creditors = self.get_items_for_disbursement('creditors', self.creditors_ids)
        balances = {}
        for entity in [*creditors, *owners]:
            balances.update({
                entity["id_"]: to_dec(entity.get("balance_after_last_disbursement", "0.00"), precision=Decimal('0.00'))
            })
        return balances

    def deactivate_payments(self):
        for payment in self.pending_payments:
            payment_pk = payments_pk.format(company_id=self.company_id)
            payment_sk = payments_sk.format(creditor_id=payment['creditor_id'], payment_id=payment['id'])
            payment_records = get_moneyflow_table().get_item(Key={'partkey': payment_pk, 'sortkey': payment_sk})['Item']
            self.rollback['deactivated_payments'].append(payment_records)
            update_inactive_entity_status(payment_pk, payment_sk, table=get_moneyflow_table)

    def del_rent_trans_from_disb_subtotal(self):
        for owner in self.owners_to_disburse:
            div_audits = [transaction.get('transaction_audit', None)
                          for transaction in owner.get("rent_transactions", [])]

            item = get_moneyflow_table().get_item(
                Key={'partkey': f'disb_subtotal_{self.company_id}', 'sortkey': f'owner_{owner["id"]}'})['Item']
            # making back up for db record
            self.rollback['disb_subtotals'].append(copy.deepcopy(item))

            rent_transactions = item.get("rent_transactions", [])
            audit = [transaction_.get('transaction_audit', None) for transaction_ in rent_transactions]
            rest = set(audit) - set(div_audits)

            result = [
                transaction_
                for transaction_ in rent_transactions
                if transaction_.get('transaction_audit', None) in [*rest]
            ]
            item["rent_transactions"] = result
            get_moneyflow_table().update_item(**db_update_item_helper(['rent_transactions'], item))
            logger.info(f'DisbursementFinalization::del_rent_trans_from_disb_subtotal: SUCCESS,'
                        f' div_audits: {div_audits},')

    def do_rollback(self):
        # do rollback for all what needs to be rolled back in self.rollback dict
        # has to have it's try-except to process its errors as no exception handling above
        message = {
            'company_id': self.company_id,
            'user_id': self.user_id,
            'rollback': self.rollback,
            'disb_id': self.disb_id
        }
        if get_debug_environment():
            DisbursementFinalization.lambda_rollback_disbursement(message, {})  # for local debug
        else:
            response = lambda_client.invoke(
                FunctionName=os.environ["LAMBDA_ROLLBACK_DISBURSEMENT"],
                InvocationType="Event",
                Payload=json.dumps(message, default=parse_decimal)
            )
            logger.info(f'DisbursementFinalization::del_rent_trans_from_disb_subtotal: do_rollback INVOKED, '
                        f'response: {response}')

    @staticmethod
    def lambda_rollback_disbursement(event: dict, context: dict):
        logger.debug(f'DisbursementFinalization::lambda_rollback_disbursement: event: {event}, context:{context}')
        event_name = 'disbursement_rollback'
        company_id = None
        disb_id = None
        try:
            event = json.loads(json.dumps(event, default=parse_decimal), parse_float=Decimal)
            company_id = event['company_id']
            disb_id = event['disb_id']
            mf_table_name_to_thread_var(company_id)
            add_pending_status(company_id, event_name)
            DisbursementFinalization.update_disb_summary_record(company_id, disb_id, {
                'status': 'rollback in progress'
            })
            rollback = event['rollback']
            batch_put_item([
                *rollback['used_transactions'],  # restore used transactions to original state
                *rollback['deactivated_payments'],  # restore status of used payments
                *rollback['entities_before_disb']['creditors']  # rollback creditors
            ], table=get_moneyflow_table)
            # rollback owners
            batch_put_item(rollback['entities_before_disb']['owners'], table=get_gen_table)
            # remove created transactions
            batch_delete_item(rollback['created_transactions'], table=get_moneyflow_table)
            # rollback rent information and balances
            time.sleep(60)
            batch_put_item(rollback['disb_subtotals'], table=get_moneyflow_table)
            DisbursementFinalization.log_disb_finalization_error(
                company_id,
                "Disbursement process was interrupted. Changes that were done during disbursement have been reversed.")
            DisbursementFinalization.update_disb_summary_record(company_id, disb_id, {
                'status': 'rolled back'
            })
        except Exception as error:
            logger.exception(f'DisbursementFinalization::lambda_rollback_disbursement: error:{error}, event:{event}')
            DisbursementFinalization.log_disb_finalization_error(
                company_id,
                f"Disbursement rollback error: {error}. Please, contact your administrator")
            DisbursementFinalization.update_disb_summary_record(company_id, disb_id, {
                'status': 'rollback error'
            })
        finally:
            delete_pending_status(company_id, event_name)

    def put_transactions_to_db(self, transactions):
        logger.debug(f'DisbursementFinalization::put_transactions_to_db: transactions:{transactions}, '
                     f'self.__dict__:{self.__dict__}')
        self.rollback['created_transactions'].extend(transactions)
        self.backup_disb_subtotal_for_entities(transactions)
        batch_put_item(transactions, table=get_moneyflow_table)

    def backup_disb_subtotal_for_entities(self, transactions):
        # getting subtotals for creditor_to_disburse
        for creditor_id in self.creditors_ids:
            item = get_moneyflow_table().get_item(
                Key={'partkey': f'disb_subtotal_{self.company_id}', 'sortkey': f'creditor_{creditor_id}'}
            ).get("Item", DisbursementFinalization.get_clean_disb_subtotal(self.company_id, creditor_id, "creditor"))
            self.rollback['disb_subtotals'].append(item)
        # getting unique records for entities which balance will be affected by new transactions
        entities_ids = [*self.owners_ids, *self.creditors_ids]
        for tr in transactions:
            if tr['ledger_type'] in ['Journal Credit', 'Receipt']:
                entity_id = tr.get('ledger_to', None)
            elif tr['ledger_type'] in ['Journal Debit', 'Payment']:
                entity_id = tr.get('ledger_from', None)
            else:
                continue

            if entity_id and entity_id not in entities_ids and tr["transaction_type"] in ['owner', 'creditor']:
                entities_ids.append(entity_id)
                item = get_moneyflow_table().get_item(Key={'partkey': f'disb_subtotal_{self.company_id}',
                                                           'sortkey': f'{tr["transaction_type"]}_{entity_id}'})['Item']
                self.rollback['disb_subtotals'].append(item)
            else:
                continue

    @staticmethod
    def get_clean_disb_subtotal(company_id: str, entity_id: str, entity_type: str) -> dict:
        return {
            "partkey": subtotal_entity_balance_record_pk.format(company_id=company_id),
            "sortkey": subtotal_entity_balance_record_sk.format(entity_type=entity_type, entity_id=entity_id),
            "company_id": company_id,
            "current_balance": 0,
            "entity_type": entity_type,
            "id_": entity_id,
            "record_type": "disbursement_subtotal"
        }

    def get_disb_transactions_to_create(self):
        logger.debug(f'DisbursementFinalization::get_disb_transactions_to_create :  self.__dict__:{self.__dict__}'
                     f'disb_datetime:{self.disb_datetime}, disb_id:{self.disb_id} ')
        transactions = []
        final_eft_transactions = []
        final_bpay_transactions = []
        for payment in self.pending_payments_db_records:
            int_transactions, bpay_transactions = self.get_pending_payment_trans_to_create(payment)
            transactions.extend(int_transactions)
            final_bpay_transactions.extend(bpay_transactions)

        for owner in self.owners_to_disburse:
            # making fees transactions for each owner for sundry_fee and management fee: from Owner, to Creditor
            if owner.get('sundry_fee', 'n/a') != 'n/a':
                transactions.extend(self.get_sundry_fees_internal_transactions(owner))
            if owner.get('properties_fee', 'n/a') != 'n/a':
                transactions.extend(self.get_management_fees_internal_transactions(owner))

            # making payout eft transactions for each owner: from Owner, to Bank
            final_eft_transactions.extend(self.get_payout_eft_transactions(owner, 'owner'))

        for creditor in self.creditors_to_disburse:
            # making payout transactions for each creditor: from Creditor, to Bank
            final_eft_transactions.extend(self.get_payout_eft_transactions(creditor, 'creditor'))

        payment_transactions = self.get_payment_trans_to_create(final_eft_transactions, final_bpay_transactions)
        return transactions, final_eft_transactions, final_bpay_transactions, payment_transactions

    @staticmethod
    def get_ledger_from_attrs(payment):
        payment_key_attrs = pending_payment_trans_type_to_ledger_from_entity_type.get(
            payment['transaction_type'].lower(), {})
        ledger_from_id = payment[payment_key_attrs.get('ledger_from_field', None)]
        entity_type_ledger_from = payment_key_attrs.get('entity_type', None)
        return entity_type_ledger_from, ledger_from_id

    def get_pending_payment_trans_to_create(self, payment):
        # making internal ledger transactions for each payment from pending payments list: from Owner, to Creditor and
        # if payment_type is BPAY then from Creditor, to Bank
        ledger_from_entity_type, ledger_from_id = self.get_ledger_from_attrs(payment)

        # Todo: for now we can process only pending payments from owner's or creditor's ledgers
        if ledger_from_entity_type in ['owner', 'creditor'] and ledger_from_id:
            entity_from_item = self.get_entity_item_by_id(ledger_from_id, ledger_from_entity_type)
            if entity_from_item:
                replace_dict_key(entity_from_item, 'id_', 'id')
                replace_dict_key(entity_from_item, 'name_', 'name')
                replace_dict_key(entity_from_item, 'creditor_name', 'name')
                int_transactions, bpay_transactions = self.get_to_creditor_transactions(
                    payment, entity_from_item, ledger_from_entity_type)
                return int_transactions, bpay_transactions
            else:
                self.log_disb_finalization_error(
                    self.company_id,
                    f'get_pending_payment_trans_to_create: '
                    f'Entity item was not found, entity type={ledger_from_entity_type}, id={ledger_from_id}')
        else:
            self.log_disb_finalization_error(
                self.company_id,
                f'get_pending_payment_trans_to_create: '
                f'One or more payments was not processed, payment={payment},'
                f'entity type={ledger_from_entity_type}, id={ledger_from_id}')
        return [], []

    def get_sundry_fees_internal_transactions(self, owner_item):
        amount = owner_item['sundry_fee']
        details = "Sundry fee"
        ledger_to = get_company_details(self.company_id, 'creditors_fees', 'sundry_fee')
        return self.get_fees_arbitrary_transactions(amount, details, ledger_to, owner_item)

    @staticmethod
    def log_disb_finalization_error(company_id, msg):
        log_sns_disb_problem(
            sub='Disbursement finalization',
            msg=msg,
            company_id=company_id
        )
        logger.critical(f"DisbursementFinalization::log_disb_finalization_error: company_id:{company_id}, msg:{msg}")

    def get_management_fees_internal_transactions(self, owner_item):
        details = "Management fee"
        ledger_to = get_company_details(self.company_id, 'creditors_fees', 'management_fee')
        management_fee_transactions = []
        for tr in owner_item.get('rent_transactions', []):
            amount = add_gst(tr["property_maintenance_fee"] + tr["property_management_fee"])
            property_id = tr.get('property_id', None)
            property_address = tr.get('property_address', '')
            tenant_id = tr.get('tenant_id', None)
            related_trans_audit = tr.get('transaction_audit', None)
            management_fee_transactions.extend(
                self.get_fees_arbitrary_transactions(
                    amount, details, ledger_to, owner_item, property_id=property_id, tenant_id=tenant_id,
                    related_trans_audit=related_trans_audit, property_address=property_address
                )
            )
        return management_fee_transactions

    @staticmethod
    def get_entity_item_by_id(entity_id, entity_type_ledger_from):
        items_from_db = query_items_by_id(
            entity_id, table=get_gen_table if entity_type_ledger_from == 'owner' else get_moneyflow_table)

        for item in items_from_db:
            if item.get('record_type') == entity_type_ledger_from:
                return item
        return None

    def get_payment_trans_to_create(self, eft_transactions_to_db, bpay_transactions_to_db):
        payment_transactions = []
        if eft_transactions_to_db:
            payment_transactions.append(self.get_payment_transaction(
                amount=get_bank_transactions_amount(eft_transactions_to_db),
                details='EFT Payment',
                ledger_from=get_company_details(self.company_id, 'banks_ids', 'eft'),
                from_name='EFT'))
        if bpay_transactions_to_db:
            payment_transactions.append(self.get_payment_transaction(
                amount=get_bank_transactions_amount(bpay_transactions_to_db),
                details='Settlement',
                ledger_from=get_company_details(self.company_id, 'banks_ids', 'bpay'),
                from_name='BPAY'))
        return payment_transactions

    def get_payment_transaction(self, amount, details, ledger_from, from_name):
        payment_trans = Transactions.init_create_transaction(
            self.company_id,
            self.user_id,
            self.add_disbursement_attributes({
                'amount': amount,
                'details': details,
                'ledger_from': ledger_from,
                'from_name': from_name,
                "transaction_type": 'Bank',
                "ledger_type": 'Payment'
            })
        ).add_db_transaction_item_attr_v2()

        payment_trans['unpresented'] = True
        return payment_trans

    def get_items_for_disbursement(self, item_type, items_ids, table=get_moneyflow_table):
        if items_ids:
            return query_items_by_id_list_v2(
                f'{item_type}_{self.company_id}',
                items_ids,
                table=table
            )
        else:
            return []

    def get_payout_eft_transactions(self, entity_item, entity_type):
        logger.debug(f"Get payout EFT transactions for {entity_item}")
        if entity_type == 'owner':
            entity_transaction_details = 'Payment to Landlord'
            entity_id = entity_item['id']
            entity_name = entity_item['name']
            entity = next(e for e in self.owners_records if e['id_'] == entity_id)
        elif entity_type == 'creditor':
            entity_transaction_details = 'Settlement'
            entity_id = entity_item['creditor_id']
            entity_name = entity_item['name']
            entity = next(e for e in self.creditors_records if e['id_'] == entity_id)
        else:
            return []
        bank_name = entity.get('bank_name', '')
        bank_bsb = entity.get('bsb', '')
        bank_account_name = entity.get('account_name', '')
        bank_account_number = str(entity.get('account_number', ''))
        bank_transaction_details = "Bank: " + bank_name + "\nAccount Name: " + bank_account_name + "\nBSB: " + \
                                   bank_bsb + "\nAccount Number: " + bank_account_number
        reference = Transactions.get_next_trans_reference_seq_num(self.company_id)
        tr_template = {
            "amount": entity_item['payment'],
            "ledger_from": entity_id,
            "ledger_to": get_company_details(self.company_id, 'banks_ids', 'eft'),
            "from_name": entity_name,
            "to_name": "EFT",
            "bank_name": bank_name,
            "bank_bsb": bank_bsb,
            "bank_account_name": bank_account_name,
            "bank_account_number": bank_account_number
        }
        return [
            Transactions.init_create_transaction(
                self.company_id,
                self.user_id,
                self.add_disbursement_attributes({
                    **tr_template,
                    "details": entity_transaction_details,
                    "transaction_type": entity_type.title(),
                    "ledger_type": 'Journal Debit'
                })
            ).add_db_transaction_item_attr_v2(reference),

            Transactions.init_create_transaction(
                self.company_id,
                self.user_id,
                self.add_disbursement_attributes({
                    **tr_template,
                    "details": bank_transaction_details,
                    "transaction_type": 'Bank',
                    "ledger_type": 'Journal Credit'
                })
            ).add_db_transaction_item_attr_v2(reference)
        ]

    def get_to_creditor_transactions(self, payment, entity_from_item, entity_from_type):
        int_transactions = []
        bpay_transactions = []
        reference = Transactions.get_next_trans_reference_seq_num(self.company_id)
        creditor_name = get_db_item(creditor_pk.format(company_id=self.company_id),
                                    payment['creditor_id'],
                                    table=get_moneyflow_table
                                    ).get('name_')
        int_tr_template = {
            "amount": payment['amount'],
            "details": payment['from_ledger'] if payment["transaction_type"] == "Property" else payment.get('details', None),
            "notes": payment.get('notes'),
            "ledger_to": payment['creditor_id'],
            "ledger_from": entity_from_item['id'],
            "from_name": entity_from_item['name'],
            "to_name": creditor_name,
            "related_property_id": payment.get('related_property_id', None),
            "property_address": payment['from_ledger'] if payment["transaction_type"] in ("Let Fee", "Property", "Advertising") else '',
            "date_": payment['date_']
        }
        int_transactions.extend([
            Transactions.init_create_transaction(
                self.company_id,
                self.user_id,
                self.add_disbursement_attributes({
                    **int_tr_template,
                    "transaction_type": entity_from_type.title(),
                    "ledger_type": 'Journal Debit'
                })
            ).add_db_transaction_item_attr_v2(reference),

            Transactions.init_create_transaction(
                self.company_id,
                self.user_id,
                self.add_disbursement_attributes({
                    **int_tr_template,
                    "transaction_type": 'Creditor',
                    "ledger_type": 'Journal Credit',
                    "crn": payment.get('crn', None) if payment.get('type_').lower() == 'bpay' else None
                })
            ).add_db_transaction_item_attr_v2(reference)
        ])

        reference = Transactions.get_next_trans_reference_seq_num(self.company_id)
        # additional functionality for bpay trans type, no else needed
        if payment.get('type_').lower() == 'bpay':
            bpay_tr_template = {
                "notes": payment.get('notes'),
                "amount": payment['amount'],
                "ledger_from": payment['creditor_id'],
                "ledger_to": get_company_details(self.company_id, 'banks_ids', 'bpay'),
                "from_name": creditor_name,
                "to_name": "BPay",
                "related_property_id": payment.get('related_property_id', None),
                "property_address": payment['from_ledger'] if payment["transaction_type"] in ("Let Fee", "Property", "Advertising") else '',
                "crn": payment.get('crn', ''),
                "biller_code": payment.get('biller_code', '')
            }

            bpay_transactions.extend([
                Transactions.init_create_transaction(
                    self.company_id,
                    self.user_id,
                    self.add_disbursement_attributes({
                        **bpay_tr_template,
                        "details": "BPay Disbursement",
                        "transaction_type": 'Creditor',
                        "ledger_type": 'Journal Debit'
                    })
                ).add_db_transaction_item_attr_v2(reference),

                Transactions.init_create_transaction(
                    self.company_id,
                    self.user_id,
                    self.add_disbursement_attributes({
                        **bpay_tr_template,
                        "details": f"Biller Code: {payment.get('biller_code', None)}\nCRN: {payment.get('crn', None)}",
                        "transaction_type": 'Bank',
                        "ledger_type": 'Journal Credit'
                    })
                ).add_db_transaction_item_attr_v2(reference)
            ])
        return int_transactions, bpay_transactions

    def get_fees_arbitrary_transactions(self, amount, details, ledger_to, owner_item, property_id=None, tenant_id=None,
                                        related_trans_audit=None, property_address=None):
        creditor = get_db_item(creditor_pk.format(company_id=self.company_id), ledger_to, table=get_moneyflow_table)
        reference = Transactions.get_next_trans_reference_seq_num(self.company_id)
        tr_template = {
            "amount": amount,
            "details": details,
            "ledger_from": owner_item['id'],
            "ledger_to": ledger_to,
            "from_name": owner_item['name'],
            "to_name": creditor['name_']
        }
        return [
            Transactions.init_create_transaction(
                self.company_id,
                self.user_id,
                self.add_disbursement_attributes({
                    **tr_template,
                    "related_property_id": property_id,
                    "related_tenant_id": tenant_id,
                    "related_trans_audit": related_trans_audit,
                    "property_address": property_address,
                    "transaction_type": 'Owner',
                    "ledger_type": 'Journal Debit'
                })
            ).add_db_transaction_item_attr_v2(reference),

            Transactions.init_create_transaction(
                self.company_id,
                self.user_id,
                self.add_disbursement_attributes({
                    **tr_template,
                    "transaction_type": 'Creditor',
                    "ledger_type": 'Journal Credit'
                })
            ).add_db_transaction_item_attr_v2(reference)
        ]

    @staticmethod
    @authenticate
    def disbursement_finalize_initial(current_request):
        # TODO: move the method to new endpoint class because there are many functions that belong only to the it
        try:
            company_id = current_request.auth_result['company_id']
            user_id = current_request.auth_result['user_id']
            disbursement = get_disbursement_record(company_id, user_id)
            check_mandatory_company_entity_ids(company_id)
            # TODO: integrate check into mandatory attrs
            assert_company_pending_event(company_id, "disbursement_finalization",
                                         "Only 1 disbursement can be launched simultaneously.")
            assert_company_pending_event(company_id, "disbursement_rollback", "Rollback is still in progress")
            message = {
                'user_id': user_id,
                'company_id': company_id
            }
            check_balances_for_disburse(disbursement, company_id)
            if get_debug_environment():
                DisbursementFinalization(message).run()  # for local debug
            else:
                response = lambda_client.invoke(
                    FunctionName=os.environ["LAMBDA_DISBURSEMENT_FINALIZATION"],
                    InvocationType="Event",
                    Payload=json.dumps(message, default=parse_decimal)
                )
                logger.info(f' DisbursementFinalization::disbursement_finalize_initial: INVOKED response:{response}')

            return Response(status_code=200, body={'message': 'request to disbursement finalization has been sent'})
        except Exception as e:
            return error_response(e)
