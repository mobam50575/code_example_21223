import calendar
import copy
import json
import os
from datetime import datetime
from uuid import uuid4

import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key
from dateutil.relativedelta import relativedelta

from n_utils.boto_clients import lambda_client
from n_utils.utils import get_gen_table, get_moneyflow_table, query_items_paged, logger, parse_decimal, timeit, \
    query_items_by_id_list_v2, to_decimal, debug_message, common_report_format_for_s3, mf_table_name_to_thread_var, \
    get_db_item, authenticate, error_response_v1, CustomResponse as Response, get_debug_environment, \
    get_current_audit_month, get_transactions_by_disb_id, relative_date_from_audit
from n_utils.keys_structure import owners_pk, creditors_pk, processed_disbursement_pk, processed_disbursement_sk, \
    payments_pk
from n_utils.email_texts import generate_default_email_body

from typing import Dict, NoReturn, List

from chalicelib.delegate_companies_class import Companies
from chalicelib.delegate_companies import get_company_details, get_company_email
from chalicelib.delegate_logger import NLogger
from chalicelib.delegate_users import get_user_email
from chalicelib.moneyflow.delegate_report import get_comma_separated_list, _get_bpay_file_no, name_reports_params, \
    send_sqs_message, push_data_to_mysql, file_names_with_sql_view, send_message
from chalicelib.moneyflow.text_file_reports import send_text_files
from chalicelib.moneyflow.delegate_statement_report import make_statement_report_v2


def generate_report_eft(eft_transactions_to_db, user_id, company_id, owner_ids, creditor_ids, email, sale=None):
    logger.debug(
        f'generate_report_eft:: eft_transactions_to_db:{eft_transactions_to_db}, user_id:{user_id}, '
        f'company_id:{company_id}, owner_ids: {owner_ids}, owner_ids:{owner_ids}: email:{email}')
    mask = ['id_', 'account_name', 'bank_name', 'bsb', 'account_number']
    creditors = [Key('partkey').eq(f'creditors_{company_id}') & Key('sortkey').eq(x) for x in creditor_ids]
    owners = [Key('partkey').eq(f'owners_{company_id}') & Key('sortkey').eq(x) for x in owner_ids]
    transactions = pd.DataFrame(eft_transactions_to_db)
    transactions['money'] = transactions['amount'].apply(lambda x: to_decimal(x))
    magic_dict = {
        'type_1': transactions,
        'owners': frame_from_complex_condition(owners, mask, table=get_gen_table),
        'creditors': frame_from_complex_condition(creditors, mask),
        # TODO pd.DataFrame([{'key': 'etf_ledger_to', 'value': get_company_details(company_id, 'banks_ids', 'eft')}]
        'constants': pd.DataFrame([{'etf_ledger_to': get_company_details(company_id, 'banks_ids', 'eft')}], index=[0]),
        'sale': pd.DataFrame(sale, index=[0], columns=mask)
    }

    creation_datetime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    pdf_report_name = 'EFT Payment Statement ' + creation_datetime
    aba_report_name = 'ABA file for EFT payment'
    report_file = 'eft_statement'
    report_type = 'eft'
    db_id = str(uuid4()).replace('-', '_')
    report_params = {
        'report_type': report_type,
        'email': email,
        'report_name': pdf_report_name,
        'creation_datetime': creation_datetime
    }
    send_to_sqs(magic_dict, company_id, user_id, report_file, pdf_report_name,
                get_comma_separated_list(email), db_id, report_type)  # generate PDF EFT report
    report_params['report_name'] = aba_report_name
    send_text_files(report_params, magic_dict, user_id, company_id)  # generate ABA EFT report


def generate_report_bpay(user_id, company_id, creditor_payment_ids, email):
    logger.debug(f'_generate_report_bpay {user_id}, {company_id}, {creditor_payment_ids}, {email}')
    mask_payments = ['from_ledger', 'crn', 'amount', 'creditor_id', 'transaction_type', 'ledger_type']
    mask_creditors = ['name_', 'biller_code', 'bsb', 'account_number', 'id_']
    payments = [
        Key('partkey').eq(f'payments_{company_id}') &
        Key('sortkey').eq(f"{x['creditor_id']}_{x['payment_id']}")
        for x in creditor_payment_ids
    ]
    creditors = [
        Key('partkey').eq(f'creditors_{company_id}') & Key('sortkey').eq(x['creditor_id'])
        for x in creditor_payment_ids
    ]

    magic_dict = {
        'payments': frame_from_complex_condition(payments, mask_payments),
        'creditors': frame_from_complex_condition(creditors, mask_creditors)
    }

    magic_dict['creditors'].drop_duplicates(subset='id_', keep='first', inplace=True)
    report_type = 'bpay'
    creation_datetime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    pdf_report_name = 'BPAY Payment Statement ' + creation_datetime
    txt_report_name = 'BPAY file'
    report_file = 'bpay_statement'
    db_id = str(uuid4()).replace('-', '_')
    report_params = {
        'report_type': report_type,
        'email': email,
        'report_name': pdf_report_name,
        'creation_datetime': creation_datetime,
        'file_no': _get_bpay_file_no()
    }
    send_to_sqs(magic_dict, company_id, user_id, report_file, pdf_report_name,
                get_comma_separated_list(email), db_id, report_type)  # generate PDF BPAY report
    report_params['report_name'] = txt_report_name
    send_text_files(report_params, magic_dict, user_id, company_id)  # generate TXT BPAY report


def _pre_sqs_message_for_report(user_email: str, report: str, year_month: str, company_id: str, user_id: str) -> Dict:
    reports_params_copy = copy.deepcopy(name_reports_params.get(report, {}))
    query_copy = reports_params_copy.get('query', {})
    query_copy['report_name'] = report
    end_date = None
    start_date = f'{year_month}-01'
    if report == 'adjustments':
        end_date = start_date
        date_minus_month = (datetime.strptime(f'{year_month}-01', '%Y-%m-%d') - relativedelta(months=1))
        start_date = f'{date_minus_month.year}-{date_minus_month.month:02d}-01'
    if query_copy:
        query_copy.update(
            email=user_email, start_date=start_date, end_date=end_date, sortkey='', disbursement_data=None,
            company_id=company_id, user_id=user_id, jrxml_templates_map=reports_params_copy.get('jrxml_templates_map'),
            partkey=query_copy.get('partkey_pattern', '').format(
                entity=query_copy.get('entity'), company_id=company_id)
        )
        return query_copy
    return dict()


def _send_reports_to_user(user_email: str, report: str, year_month: str, company_id: str, user_id: str) -> NoReturn:
    debug_message(f'_send_reports_to_user:: user_email:{user_email}, report:{report}, year_month:{year_month}, '
                  f'company_id:{company_id}, user_id:{user_id}')
    if report:
        query = _pre_sqs_message_for_report(user_email, report, year_month, company_id, user_id)
        if query:
            try:
                # _sqs_report_handler(query)  # for debug purpose only
                send_sqs_message(os.environ["REPORT_HANDLER_QUEUE_URL"], query)

            except Exception as error:
                logger.exception(f'_send_reports_to_user:: error:{error}, report:{report}, query:{query}')
        else:
            logger.info(f'_send_reports_to_user:: NOT SUPPORTED CASE, query: None, report:{report}')
    else:
        logger.info(f'_send_reports_to_user:: NOT SUPPORTED CASE, report:None')


def make_user_reports(company_id, user_id, trust_account_reports, user_email):
    year_month = get_current_audit_month(company_id)
    for report in trust_account_reports:
        _send_reports_to_user(user_email, report, year_month, company_id, user_id)


@timeit
def make_statement_reports(disb_id, company_id, entity_ids, company_email,
                           payments, entities_balances_after_last_disb):
    date_ = datetime.now().strftime("%d/%m/%Y")

    yearmonth = get_current_audit_month(company_id)
    owners_list, creditors_list = get_statement_entities(entity_ids, company_id)
    extra_params = {'disbursement_id': disb_id}

    invoke_message = {
        "company_id": company_id,
        "yearmonth": yearmonth,
        "company_email": company_email,
        "payments": payments,
        "params": extra_params,
        "date_": date_
    }

    for owner in owners_list:
        invoke_message.update({
            "entity": owner,
            "report_params": name_reports_params['statement_owner_xml'],
            "entity_balance": entities_balances_after_last_disb[owner['id_']]
        })
        if get_debug_environment():
            make_statement_report_v2(**invoke_message)  # for local debug
        else:
            lambda_client.invoke(
                FunctionName=os.environ["LAMBDA_STATEMENT_REPORT"],
                InvocationType="Event",
                Payload=json.dumps(invoke_message, default=parse_decimal)
            )

    for creditor in creditors_list:
        invoke_message.update({
            "entity": creditor,
            "report_params": name_reports_params['statement_creditor_xml'],
            "entity_balance": entities_balances_after_last_disb[creditor['id_']]
        })
        if get_debug_environment():
            make_statement_report_v2(**invoke_message)  # for local debug
        else:
            lambda_client.invoke(
                FunctionName=os.environ["LAMBDA_STATEMENT_REPORT"],
                InvocationType="Event",
                Payload=json.dumps(invoke_message, default=parse_decimal)
            )


def frame_from_complex_condition(
        multi_key_condition_expression: List[boto3.dynamodb.conditions.And],
        projection_expression: List[str], table=get_moneyflow_table):
    res = []
    columns = projection_expression
    projection_expression = ', '.join(projection_expression)
    for key_condition_expression in multi_key_condition_expression:
        res.extend(query_items_paged(
            key_condition_expression=key_condition_expression,
            projection_expression=projection_expression, table=table)
        )
    return pd.DataFrame(res, columns=columns)


def send_to_sqs(magic_dict, company_id, user_id, report_file, report_name, email, db_id=None, report_type=None):
    # TODO REMOVE
    if not db_id:
        db_id = str(uuid4()).replace('-', '_')

    push_data_to_mysql(db_id, magic_dict, file_names_with_sql_view['transactions'])
    extra = {}
    prefix_report_file = os.environ['PREFIX_REPORT_FILE']
    month = calendar.month_name[datetime.today().month]
    file_name = common_report_format_for_s3(company_id, user_id, report_type, report_name, '')
    subject = report_name + " from n_company"
    email_body = generate_default_email_body(
        report_name=report_name,
        company_name=Companies.get_company_name(company_id),
        file_key=f'{file_name}.pdf'
    )
    send_message(
        jrxml_templates_map={"pdf": f'{prefix_report_file}{report_file}.jrxml'},
        db_name=db_id,
        file_name=file_name,
        email=email,
        month=month,
        report_name=report_name,
        subject=subject,
        company_id=company_id,
        email_body=email_body,
        **extra
    )


def get_statement_entities(entity_ids, company_id):
    owners = query_items_by_id_list_v2(
        owners_pk.format(company_id=company_id),
        entity_ids.get('eft_owners_ids')
    )

    creditors = query_items_by_id_list_v2(
        creditors_pk.format(company_id=company_id),
        entity_ids.get('eft_creditors_ids'),
        table=get_moneyflow_table
    )
    return owners, creditors


class DisbursementReporting:

    def __init__(self, company_id, disb_id):
        logger.debug(f'DisbursementReporting init company_id: {company_id}, disb_id: {disb_id}')
        self.company_id = company_id
        self.disb_id = disb_id
        self.disb_summary = get_db_item(partkey=processed_disbursement_pk.format(company_id=self.company_id),
                                        sortkey=processed_disbursement_sk.format(disbursement_id=self.disb_id))
        # emails
        self.user_id = self.disb_summary['user_id']
        self.email = get_user_email(self.user_id)
        self.company_email = get_company_email(self.company_id)
        self.py_lambda_email_list = [self.email, self.company_email]
        # entities' ids
        self.entities_to_disburse_ids = self.disb_summary.get("entities_to_disburse_ids", [])

    def do_reporting(self):
        logger.debug(
            f'DisbursementFinalization::do_reporting: '
            f'disb_id: {self.disb_id}, entities_to_disburse_ids:{self.entities_to_disburse_ids}, '
            f'eft_transaction_audits:{self.disb_summary.get("eft_transaction_audits", [])}, '
            f'bpay_transaction_audits:{self.disb_summary.get("bpay_transaction_audits", [])}, '
            f'self.__dict__:{self.__dict__}'
        )
        if self.disb_summary.get("eft_transaction_audits", []):
            generate_report_eft(
                self.get_eft_transactions(),
                self.user_id,
                self.company_id,
                self.entities_to_disburse_ids.get('eft_owners_ids', []),
                self.entities_to_disburse_ids.get('eft_creditors_ids', []),
                self.py_lambda_email_list
            )
        if self.disb_summary.get("bpay_transaction_audits", []):
            generate_report_bpay(
                self.user_id,
                self.company_id,
                self.entities_to_disburse_ids['bpay_creditor_payment_ids'],
                self.py_lambda_email_list
            )

        make_user_reports(self.company_id, self.user_id, self.disb_summary.get('requested_trust_account_reports', []),
                          get_comma_separated_list(self.py_lambda_email_list))

        make_statement_reports(
            self.disb_id, self.company_id, self.entities_to_disburse_ids, self.company_email,
            self.get_disb_related_payments(), self.disb_summary.get("entities_balances_after_last_disb", {})
        )

    def get_eft_transactions(self):
        year_month = relative_date_from_audit(str(self.disb_summary['audit_partition']))
        disb_trs = get_transactions_by_disb_id(self.company_id, year_month, self.disb_id)
        eft_trans = [tr for tr in disb_trs if tr['audit'] in self.disb_summary["eft_transaction_audits"]]
        return eft_trans

    def get_disb_related_payments(self):
        return query_items_by_id_list_v2(
            payments_pk.format(company_id=self.company_id),
            self.disb_summary["pending_payment_records_ids"])

    @staticmethod
    def lambda_disbursement_reporting(event: dict, context: dict):
        logger.debug(f'DisbursementReporting::lambda_disbursement_reporting: event: {event}, context:{context}')
        company_id = None
        try:
            company_id = event['company_id']
            disb_id = event['disbursement_id']
            mf_table_name_to_thread_var(company_id)
            DisbursementReporting(company_id, disb_id).do_reporting()
        except Exception as error:
            msg = f'DisbursementReporting::lambda_disbursement_reporting: error:{error}, event:{event}'
            logger.exception(msg)
            NLogger.log_sns('DisbursementReporting Problem:', msg,
                               subsystem_origin='DisbursementReporting',
                               company_id=company_id)


@authenticate
def request_disbursement_reports(current_request, disbursement_id):
    try:
        company_id = current_request.auth_result['company_id']
        message = {"company_id": company_id, "disbursement_id": disbursement_id}
        processed_disbursement = get_db_item(partkey=processed_disbursement_pk.format(company_id=company_id),
                                             sortkey=processed_disbursement_sk.format(disbursement_id=disbursement_id))
        assert processed_disbursement['status'] == 'processed', (f"Wrong status of the processed disbursement: "
                                                                 f"{processed_disbursement['status']}")
        if get_debug_environment():
            DisbursementReporting(company_id, disbursement_id).do_reporting()
        else:
            message = {"company_id": company_id, "disbursement_id": disbursement_id}
            response = lambda_client.invoke(
                FunctionName=os.environ["LAMBDA_DISBURSEMENT_REPORTING"],
                InvocationType="Event",
                Payload=json.dumps(message)
            )
            logger.info(f'DisbursementFinalization:: run: LAMBDA_DISBURSEMENT_REPORTING INVOKED, '
                        f'response: {response}')
        logger.debug(f'request_disbursement_records:: SUCCESS: message: {message}')
        return Response(status_code=200, body={f"Disbursement report {disbursement_id} was successfully requested"})
    except Exception as e:
        logger.debug(f'request_disbursement_records:: Exception {e}')
        return error_response_v1(e, 500)
