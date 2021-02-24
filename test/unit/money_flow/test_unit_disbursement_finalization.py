import threading
from decimal import Decimal

import boto3
import pytest
import xmltodict
from n_utils import utils

from chalicelib.moneyflow import delegate_statement_report
from chalicelib.moneyflow.delegate_statement_report import make_statement_report_v2


def asserts(*args):
    url, message = args
    s3 = boto3.resource('s3')
    obj = s3.Object(message['pdfFileLocation'], message['dataFileKey'])
    body = obj.get()['Body'].read()
    root = xmltodict.parse(body)['root']
    boto3.resource('s3').Object(message['pdfFileLocation'], message['dataFileKey']).delete()
    return [*root.keys()] == ['company', 'owner', 'tenants', 'close_balances', 'transactions']


@pytest.mark.local_db_test
def test_make_owner_statement_report2(monkeypatch):
    monkeypatch.setenv('DEBUG', 'true')
    local_db = 'local'
    local_tread = threading.local()
    local_tread.mf_table_name = local_db
    monkeypatch.setattr(utils.environment_utils, 'thread_vars', local_tread)
    monkeypatch.setattr(delegate_statement_report, 'send_sqs_message', lambda *x, **y: asserts(*x))
    invoke_message = {
        'company_id': 'd628e10a-d420-4d39-94af-11f7652e0f8d',
        'yearmonth': '2020-12',
        'company_email': 'success@simulator.amazonses.com',
        'payments': [{
            'company_id': 'd628e10a-d420-4d39-94af-11f7652e0f8d',
            'id_': '03d32cef-bdd9-4e33-b805-ba4299a866ee',
            'creditor_id': 'edaea2df-942b-4c30-bf13-d6810d2218fe',
            'file': 'f35cbba4-294b-4324-a0b2-bd1cc6c6ff74',
            'gst': False,
            'account_number': '147852369',
            'create_date': Decimal('1608047802069'),
            'account_name': 'BigLebowski',
            'inactive': True, 'bsb': '613-613',
            'user_id': '43b8283e-d191-4722-9db2-828ff3999b7a',
            'date_': '2020-12-15', 'type_': 'EFT',
            'sortkey': 'edaea2df-942b-4c30-bf13-d6810d2218fe_03d32cef-bdd9-4e33-b805-ba4299a866ee',
            'deposit': 'False',
            'from_ledger': 'Lebowsky',
            'partkey': 'payments_d628e10a-d420-4d39-94af-11f7652e0f8d',
            'amount': '400',
            'transaction_type': 'Owner',
            'record_type': 'payment'
        }, {
            'company_id': 'd628e10a-d420-4d39-94af-11f7652e0f8d',
            'id_': '54760d3d-7340-4b47-80a0-a7465d426dc4',
            'creditor_id': 'edaea2df-942b-4c30-bf13-d6810d2218fe',
            'file': 'f35cbba4-294b-4324-a0b2-bd1cc6c6ff74',
            'gst': False,
            'account_number': '147852369',
            'create_date': Decimal('1608041480543'),
            'account_name': 'BigLebowski',
            'inactive': True, 'bsb': '613-613',
            'user_id': '43b8283e-d191-4722-9db2-828ff3999b7a',
            'date_': '2020-12-15', 'type_': 'EFT',
            'sortkey': 'edaea2df-942b-4c30-bf13-d6810d2218fe_54760d3d-7340-4b47-80a0-a7465d426dc4',
            'deposit': 'False',
            'from_ledger': 'Lebowsky',
            'partkey': 'payments_d628e10a-d420-4d39-94af-11f7652e0f8d',
            'amount': '420',
            'transaction_type': 'Owner',
            'record_type': 'payment'
        }],
        'params': {'disbursement_id': '106b88a1-d8d1-4fa1-9f9a-df53739dcc20'}, 'date_': '22/12/2020', 'entity': {
            'company_id': 'd628e10a-d420-4d39-94af-11f7652e0f8d', 'id_': 'f35cbba4-294b-4324-a0b2-bd1cc6c6ff74',
            'properties': [], 'last_payout_date': '2020-12-16', 'email': 'nn@tn_utils.com',
            'bank_name': 'BANK SA', 'name': 'Jeffrey "The Big" Lebowski', 'create_date': Decimal('1608041147025'),
            'account_name': 'LEBOWSKI', 'balance_after_last_disbursement': Decimal('420'),
            'disbursement_series': 'End of Month', 'sundry_fee': '19.09', 'bsb': '450-540',
            'sortkey': 'f35cbba4-294b-4324-a0b2-bd1cc6c6ff74', 'phone': '+1053446725', 'name_': 'Lebowski Baby',
            'account_number': '491616176', 'address': '81 Stearns Drive',
            'user_id': '43b8283e-d191-4722-9db2-828ff3999b7a', 'last_payout_audit': '110.01484',
            'partkey': 'owners_d628e10a-d420-4d39-94af-11f7652e0f8d', 'statement_number': Decimal('4'),
            'withhold': '420',
            'record_type': 'owner', 'update_date': Decimal('1608041147025')
        }, 'report_params': {
            'query': {
                'report_name': 'Landlord Statement', 'datasource_type': 'XML', 'partkey_pattern': '',
                'report_type': 'entity', 'query_params': {'entity_type': 'owner', 'entities_types': ['tenants']}
            }, 'jrxml_templates_map': {'pdf': 'report_from_15_12_2020_AF_statement_owner_xml.jrxml'},
            'add_report_request_date': False
        }, 'entity_balance': 0
    }
    assert make_statement_report_v2(**invoke_message), 'Not all fields are present '
