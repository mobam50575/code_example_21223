import json
import sys
from uuid import uuid4

import jwt
from decimal import Decimal

import mock
import pytest
from n_utils.boto_clients import lambda_client
from n_utils.utils import get_moneyflow_table

import chalicelib
from chalicelib.moneyflow.delegate_report import name_reports_params
from chalicelib.moneyflow.disbursement import disbursement_finalization_class
from chalicelib.moneyflow.disbursement.delegate_disbursement import add_db_disbursement_item_attr
from chalicelib.moneyflow.disbursement.disbursement_finalization_class import DisbursementFinalization, \
    get_bank_transactions_amount
from test.test_utils.common_raises import http200
from test.test_utils.fixtures import create_transaction_object, chalice_gateway, token_id, rnd_db_user, rnd_company, \
    rnd_owner, rnd_disbursement_subtotal_owner


@pytest.mark.local_db_test
def test_get_transactions_amount():
    tenant = str(uuid4())
    owner = str(uuid4())
    user_id = str(uuid4())
    company_id = str(uuid4())
    transactions = [
        create_transaction_object(tenant, owner, user_id, company_id,
                                  '101', '00505', transaction_type='Bank', amount=Decimal(230)),
        create_transaction_object(tenant, owner, user_id, company_id, '101', '00506', amount=Decimal(450)),
        create_transaction_object(tenant, owner, user_id, company_id,
                                  '101', '00507', transaction_type='Bank', amount=Decimal(111))]
    amount = get_bank_transactions_amount(transactions)
    assert amount == Decimal(341)


def request_for_disbursement_finalize_initial(chalice_gateway, token_id):
    return chalice_gateway.handle_request(
        method='POST',
        path='/disbursement/finalize',
        headers={
            'Content-Type': 'application/json',
            'Authorization': token_id,
            'Accept': 'application/pdf, application/octet-stream'
        },
        body='')


class TestRequestEntityTooLargeException(Exception):
    """Test class too large size
    """


class LambdaInvokeTest:
    @classmethod
    def invoke(cls, *args, **kwargs):
        if sys.getsizeof(kwargs['Payload'])//1024 > 255:
            raise TestRequestEntityTooLargeException
        return http200


@pytest.mark.first_prior_local_db_test
@mock.patch('chalicelib.moneyflow.disbursement.disbursement_finalization_class.check_mandatory_company_entity_ids')
def test_disbursement_finalize_initial(_check_company, chalice_gateway, token_id, monkeypatch, rnd_company):
    decoded_jwt_token = jwt.decode(token_id, '', algorithms=['RS256'], verify=False)
    company_id = decoded_jwt_token.get('custom:company_id', None)
    user_id = decoded_jwt_token.get('sub', None)
    disbursement_record = add_db_disbursement_item_attr({}, user_id, company_id)
    get_moneyflow_table().put_item(Item=disbursement_record)

    response = request_for_disbursement_finalize_initial(chalice_gateway, token_id)
    assert response['statusCode'] == 200

    _check_company.side_effect = Exception()
    response = request_for_disbursement_finalize_initial(chalice_gateway, token_id)
    assert response['statusCode'] == 500


# DPopov
# the test it works incorrectly check_balances_for_disburse
@pytest.mark.skip
@pytest.mark.first_prior_local_db_test
def test_disbursement_finalize_requested_payment_greater_than_available_for_payment(
        chalice_gateway, token_id, rnd_owner, rnd_disbursement_subtotal_owner):
    owner = rnd_owner['owner']
    owner.update({'id': owner['id_']})

    decoded_jwt_token = jwt.decode(token_id, '', algorithms=['RS256'], verify=False)
    company_id = decoded_jwt_token.get('custom:company_id', None)
    user_id = decoded_jwt_token.get('sub', None)
    disbursement_record = add_db_disbursement_item_attr({}, user_id, company_id)
    disbursement_record.update({'owners_to_disburse': [owner]})
    get_moneyflow_table().put_item(Item=disbursement_record)

    response = request_for_disbursement_finalize_initial(chalice_gateway, token_id)
    response_body = json.loads(response['body'])
    assert response['statusCode'] == 500
    assert response_body['message'] == f'Requested payment for disbursement item (id = {owner["id_"]}) ' \
                                       f'doesn\'t equal available balance for_payment'


disbursement = {
    'company_id': '5f771649-6e8c-4442-a1b0-212b1b7bf0a7',
    'payments': [{
        'type': 'EFT',
        'creditor_id': '3bedfaae-9007-4b0c-bc8e-c672541ca312',
        'id': '26c5ee3e-f630-4e31-985f-b4c3ac2a13a8'
    }] * 1024,
    'user_id': '47e9719a-68c0-4438-b59f-bcd48bb8c0e7',
    'date_': '2021-02-09',
    'disbursement_is_completed': True,
    'sortkey': '47e9719a-68c0-4438-b59f-bcd48bb8c0e7',
    'owners_balances': [{
        'properties_fee': 0, 'fees': 5.5,
        'company_id': '5f771649-6e8c-4442-a1b0-212b1b7bf0a7',
        'bank_details': {
            'bsb': '306-089',
            'bank_name': 'BANKWEST',
            'account_number': 1794096,
            'account_name': 'Mr G & Mrs D Steyn'
        }, 'payments': [], 'available': 877,
        'payments_sum': 0,
        'rent_transactions': [], 'hold': 0,
        'withhold': 0,
        'entity_type': 'owner',
        'sundry_fee': 5.5,
        'disbursement_series_y': 'Weekly',
        'disbursement_series': 'Weekly',
        'name': 'Gary Steyn',
        'payment': 871.5,
        'id': '01b0a5ec-b69c-4a1d-961b-ee827072a1ef'
    }] * 1024,
    'partkey': 'disbursement_5f771649-6e8c-4442-a1b0-212b1b7bf0a7',
    'creditors_balances': [{
        'creditor_id': '0ca158db-3cab-4b16-a64a-0ad77bec6dd1',
        'fees': 0,
        'withhold': 0,
        'bank_details': {
            'bsb': '013-606',
            'bank_name': 'ANZ',
            'account_number': 527158584,
            'account_name': 'UPM - Sales Commission'
        },
        'available': 15511.46,
        'name': 'UPM - Sales Commission',
        'payment': 15511.46,
        'hold': 0
    }] * 1024,
    'creditors_to_disburse': [{
        'creditor_id': '1ddc0b44-a527-428b-a82c-fe3e909ae275', 'fees': 0, 'withhold': 0,
        'bank_details': {
            'bsb': '684-444', 'bank_name': 'TCU', 'account_number': '984655656',
            'account_name': 'Stocks'twig
        }, 'available': 100, 'name': 'Mister Stocks', 'checked': True, 'index': 2,
        'payment': 100, 'hasError': False, 'hold': 0
    }] * 1024,
    'owners_to_disburse': [{
        'properties_fee': 0, 'fees': 5.5,
        'company_id': '5f771649-6e8c-4442-a1b0-212b1b7bf0a7',
        'bank_details': {
            'bsb': '306-089', 'bank_name': 'BANKWEST',
            'account_number': 1794096,
            'account_name': 'Mr G & Mrs D Steyn'
        }, 'payments': [], 'available': 877,
        'payments_sum': 0, 'index': 0,
        'rent_transactions': [], 'hold': 0, 'withhold': 0,
        'entity_type': 'owner', 'sundry_fee': 5.5,
        'disbursement_series_y': 'Weekly',
        'disbursement_series': 'Weekly', 'name': 'Gary Steyn',
        'checked': True, 'payment': 871.5,
        'id': '01b0a5ec-b69c-4a1d-961b-ee827072a1ef',
        'hasError': False
    }] * 1024,
    'record_type': 'disbursement'
}


@pytest.mark.first_prior_local_db_test
@mock.patch.object(lambda_client, 'invoke', lambda *args, **kwargs: LambdaInvokeTest().invoke(*args, **kwargs))
def test_disbursement_finalize_initial_error_request_to_large(chalice_gateway, token_id, monkeypatch, rnd_company):
    monkeypatch.setenv('DEBUG', 'false')
    monkeypatch.setattr(disbursement_finalization_class, 'get_disbursement_record', lambda *x, **y: disbursement)
    monkeypatch.setattr(disbursement_finalization_class, 'check_mandatory_company_entity_ids', lambda *x, **y: None)
    monkeypatch.setattr(disbursement_finalization_class, 'assert_company_pending_event', lambda *x, **y: None)
    monkeypatch.setattr(disbursement_finalization_class, 'check_balances_for_disburse', lambda *x, **y: None)
    response = request_for_disbursement_finalize_initial(chalice_gateway, token_id)
    assert response['statusCode'] == http200
