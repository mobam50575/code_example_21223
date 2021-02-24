import os
import uuid

from datetime import datetime

from boto3.dynamodb.conditions import Key, Attr, And
from dicttoxml import dicttoxml, LOG
from n_utils.utils import substitute_keys_from_db, query_items_paged, get_moneyflow_table, logger, \
    money_flow_query_paged, upload_file_to_s3, remove_na_from_entity_record, add_month, timeit, debug_message, \
    get_current_audit_month
from n_utils.keys_structure import transactions_pk as tr_pk, closebalance_pk

from chalicelib.delegate_companies_class import Companies
from chalicelib.moneyflow.delegate_close_balance import get_entity_close_balance_v2


def save_entity_xml_on_s3(company_id, query_params, entity_id, *args, **kwargs):
    """ wrapper for convenient use in associative arrays """
    return ReportData(company_id, query_params, entity_id).generate_xml_data_for_entity()


def save_entities_xml_on_s3(company_id, query_params, *args, **kwargs):
    """ wrapper for convenient use in associative arrays """
    return ReportData(company_id, query_params).generate_xml_data_for_entities()


class ReportData:
    """ Class to get data in xml or json format for general reports and previews. """

    def __init__(self, company_id, query_params, entity_id=None):
        logger.debug(f"XMLReportData init company_id: {company_id}")
        self.company_id = company_id
        self.entity_type = query_params.get("entity_type")
        self.entity_id = entity_id
        self.get_entity_func = {
            'tenants': self.query_all_tenants_func,
            'properties': lambda: query_items_paged(Key('partkey').eq(f'properties_{self.company_id}')),
            'owners': lambda: query_items_paged(Key('partkey').eq(f'owners_{self.company_id}')),
            'creditors': lambda: query_items_paged(Key('partkey').eq(f'creditors_{self.company_id}')),
            'sales': lambda: query_items_paged(Key('partkey').eq(f'sales_{self.company_id}')),
            'payments': lambda: query_items_paged(Key('partkey').eq(f'payments_{self.company_id}')),
            'banks': lambda: query_items_paged(Key('partkey').eq(f'banks_{self.company_id}'))
        }
        self.entities_types = query_params.get("entities_types", [])
        self.date_start = query_params.get("date_start")
        self.date_end = query_params.get("date_end") or f"{get_current_audit_month(self.company_id)}-01"
        self.entities_data = {}
        LOG.disabled = True

    def query_all_tenants_func(self):
        return money_flow_query_paged(Key('gsi_get_all_tenants_partkey').eq(f'tenants_{self.company_id}'),
                                      index_name='gsi_get_all_tenants_partkey-index')

    @timeit
    def generate_xml_data_for_entities(self):
        """ saves xml with data for entities on S3 """
        self.prepare_report_data_for_entities()
        now = datetime.now()
        xml = dicttoxml(self.entities_data)
        debug_message(f'timeit:: function : dicttoxml time is {datetime.now() - now}')
        file_key = f'xml_data/{str(uuid.uuid4())}_report_data.xml'
        upload_file_to_s3(os.environ["REPORTS_BUCKET"], xml, file_key, 'application/xml')
        return file_key

    def prepare_report_data_for_entities(self):
        self.entities_data["company"] = Companies.get_full_company_details(self.company_id)
        logger.info(f"ReportData::prepare_report_data_for_entities: "
                    f"self.entities_data['company']:{self.entities_data['company']}")
        self.get_entities_data()
        self.get_transactions_and_close_balances_for_period()

    def get_entities_data(self):
        for entity_type in self.entities_types:
            entities_records = self.get_entity_func[entity_type]()
            self.add_clean_data_to_entities_dict(entities_records, entity_type)
        logger.info(f"ReportData:: get_entities_data : self.entities_types: {self.entities_types}")

    def get_transactions_and_close_balances_for_period(self):
        list_of_year_month = self.create_list_of_audit_dates()
        self.get_close_balances_for_entities()
        transactions = []
        query_expression = {'table': get_moneyflow_table}
        for year_month in list_of_year_month:
            query_key = Key('partkey').eq(tr_pk.format(company_id=self.company_id, year_month=year_month))
            query_expression.update({'key_condition_expression': query_key})
            transactions.extend(query_items_paged(**query_expression))

        self.add_clean_data_to_entities_dict(transactions, "transactions")

    def create_list_of_audit_dates(self):
        # if start_date is bigger than audit date there is possibility of not closed audit month
        # and we need to take audit month to get any transaction at all. Format: %Y-%m-%d
        self.date_start = min((self.date_start or self.date_end), self.date_end)  # By default date_end is current audit month
        date_end = self.date_end[0:7]
        list_of_year_month = [self.date_start[0:7]]  # At least 1 year-month shall be taken: current audit month
        # We create list of all audit periods
        while list_of_year_month[-1] < date_end:
            dt = add_month(datetime.strptime(list_of_year_month[-1], "%Y-%m"))  # parse date from %Y-%m and add 1 month
            list_of_year_month.append(dt.strftime("%Y-%m"))  # append audit period to list

        return list_of_year_month

    def get_close_balances_for_entities(self):
        # getting close balance for requested period
        cb_yearmonth = add_month(datetime.strptime(self.date_start, "%Y-%m-%d"), month=-1).strftime("%Y-%m")
        close_balances = query_items_paged(
            Key('partkey').eq(closebalance_pk.format(company_id=self.company_id, year_month=cb_yearmonth)),
            table=get_moneyflow_table
        )
        self.add_clean_data_to_entities_dict(close_balances, "close_balances")

    @timeit
    def generate_xml_data_for_entity(self):
        """ saves xml with data for entity on S3 """
        self.prepare_report_data_for_entity()
        now = datetime.now()
        xml = dicttoxml(self.entities_data)
        debug_message(f'timeit:: function : dicttoxml time is {datetime.now() - now}')
        file_key = f'xml_data/{str(uuid.uuid4())}_report_data.xml'
        logger.debug(f'generate_xml_data_for_entity: file_key: {file_key}')
        upload_file_to_s3(os.environ["REPORTS_BUCKET"], xml, file_key, 'application/xml')
        return file_key

    def prepare_report_data_for_entity(self):
        self.entities_data["company"] = Companies.get_full_company_details(self.company_id)
        logger.debug(f"entities_data['company']: {self.entities_data['company']}")
        self.get_entity_data()
        self.get_entities_data()
        self.get_transactions_and_close_balance_for_entity()

    def get_entity_data(self):
        logger.debug(f"entities_ids: {self.entity_id}")
        now = datetime.now()
        entity_record = query_items_paged(
            Key('id_').eq(self.entity_id),
            filter_expression=Attr("record_type").eq(self.entity_type),
            index_name="id_-index",
            table=get_moneyflow_table
        )
        debug_message(f'timeit:: function : get_entity_data query_items_paged time is {datetime.now() - now}')
        self.add_clean_data_to_entities_dict(entity_record, self.entity_type)

    def get_transactions_and_close_balance_for_entity(self):
        self.date_start = self.date_start or "1970-02-01"  # if there is no date_start, we need all transactions
        self.get_close_balance_for_entity()

        filter_expression = And(Attr('audit_yyyymm').gte(self.date_start[0:7]),
                                Attr('audit_yyyymm').lte(self.date_end[0:7]))
        query_expression: dict = {'table': get_moneyflow_table,
                                  'filter_expression': filter_expression,
                                  'key_condition_expression': Key('ledger_from').eq(self.entity_id),
                                  'index_name': 'ledger_from-index'}
        # get all transaction that comes from entity_id
        transactions: list = query_items_paged(**query_expression)
        # get all transaction that comes to entity_id
        query_expression.update({'key_condition_expression': Key('ledger_to').eq(self.entity_id),
                                 'index_name': 'ledger_to-index'})
        transactions.extend(query_items_paged(**query_expression))

        # We can have transactions from entity to itself. We must handle it by removing copies.
        transactions = [tr for n, tr in enumerate(transactions) if tr not in transactions[n + 1:]]
        self.add_clean_data_to_entities_dict(transactions, "transactions")

    def get_close_balance_for_entity(self):
        # getting close balance for requested period
        cb_yearmonth = add_month(datetime.strptime(self.date_start, "%Y-%m-%d"), month=-1).strftime("%Y-%m")
        close_balances: list = [get_entity_close_balance_v2(self.company_id, self.entity_id, cb_yearmonth)]
        self.add_clean_data_to_entities_dict(close_balances, "close_balances")

    def add_clean_data_to_entities_dict(self, entities_records: list, entity_type: str):
        for entity_record in entities_records:
            substitute_keys_from_db(entity_record)
            remove_na_from_entity_record(entity_record)
        self.entities_data.update({entity_type: entities_records})
        logger.debug(f"entities_data: {self.entities_data}")
