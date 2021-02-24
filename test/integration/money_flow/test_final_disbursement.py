from random import choices

import mock
from n_utils.utils import parse_decimal, expression_preparation

from chalicelib.moneyflow.disbursement.delegate_disbursement import get_disbursement_record
from chalicelib.moneyflow.disbursement.disbursement_finalization_class import DisbursementFinalization
from test.integration.money_flow.test_delegate_transactions import request_create_transaction_class, gen_transaction
from test.test_utils.disbursement_finalization_utils import __sub_test_create_disbursement, \
    __sub_test_item_get_subtotal_disbursement_created, __sub_test_update_disbursement, __sub_test_update_payments, \
    __sub_test_prepare_owners_disbursement, __sub_test_prepare_creditors_disbursement, \
    __sub_test_get_items_for_disbursement, __sub_test_disbursement_finalize, __is_disbursement_ready, \
    __is_owner_subtotal_ready, __is_creditor_subtotal_ready, create_ready_owner_disbursement, get_disb_data, \
    __is_disbursement_done, assert_disbursement_data, call_lambda_rollback_disbursement, call_make_statement_report_v2
from test.test_utils.fixtures import *


@pytest.mark.master_parallel_test
def test_zero_balance_disbursement_for_owner(rnd_token_id, rnd_company_v2, rnd_disbursement, chalice_gateway):
    company_id = rnd_disbursement['owners'][0]['company_id']
    token = rnd_disbursement['token']
    user_id = rnd_disbursement['user_id']
    __sub_test_create_disbursement(token, chalice_gateway)
    __sub_test_item_get_subtotal_disbursement_created(company_id, 'owner')
    __sub_test_update_disbursement(token, chalice_gateway, {"date": "2020-05-07"}, company_id, user_id)
    __sub_test_update_payments(
        token, chalice_gateway, {"payments": rnd_disbursement['payments']}, company_id, user_id)
    __sub_test_prepare_owners_disbursement(token, chalice_gateway)
    owner = __sub_test_get_items_for_disbursement(token, chalice_gateway, "owners_balances")[0]
    __sub_test_update_disbursement(
        token, chalice_gateway, {"owners_to_disburse": [owner]}, company_id, user_id)
    __sub_test_update_disbursement(token, chalice_gateway, {"creditors_to_disburse": []}, company_id, user_id)
    __sub_test_update_disbursement(token, chalice_gateway, {"trust_account_reports": []}, company_id, user_id)
    __is_disbursement_ready(company_id, user_id, owners_len=1, payments_len=len(rnd_disbursement['payments']))
    event_110_balance = WaiterEvent(__is_owner_subtotal_ready, company_id=company_id, owner=owner, value=110)
    event_zero_balance = WaiterEvent(__is_owner_subtotal_ready, company_id=company_id, owner=owner, value=0)
    assert event_110_balance()
    __sub_test_disbursement_finalize(token, chalice_gateway)
    assert event_zero_balance()


@pytest.mark.master_parallel_test
def test_zero_balance_disbursement_for_creditor(rnd_token_id, rnd_company_v2, rnd_disbursement, chalice_gateway):
    # TODO TEST LOOKS LIKE POSSIBLE TO PARAMETERIZE
    company_id = rnd_disbursement['owners'][0]['company_id']
    token = rnd_disbursement['token']
    user_id = rnd_disbursement['user_id']
    __sub_test_create_disbursement(token, chalice_gateway)
    __sub_test_item_get_subtotal_disbursement_created(company_id, 'creditor')
    __sub_test_update_disbursement(token, chalice_gateway, {"date": "2020-05-07"}, company_id, user_id)
    __sub_test_update_disbursement(token, chalice_gateway, {"payments": []}, company_id, user_id)
    __sub_test_prepare_creditors_disbursement(token, chalice_gateway)
    creditors = __sub_test_get_items_for_disbursement(token, chalice_gateway, "creditors_balances")
    creditor = sorted(creditors, key=lambda i: i['available'], reverse=True)[0]  # expected 200 available
    __sub_test_update_disbursement(token, chalice_gateway, {"owners_to_disburse": []}, company_id, user_id)
    __sub_test_update_disbursement(
        token, chalice_gateway, {"creditors_to_disburse": [creditor]}, company_id, user_id)
    __sub_test_update_disbursement(token, chalice_gateway, {"trust_account_reports": []}, company_id, user_id)
    __is_disbursement_ready(company_id, user_id, creditors_len=1)
    event_200_balance = WaiterEvent(__is_creditor_subtotal_ready, company_id=company_id, creditor=creditor, value=200)
    event_zero_balance = WaiterEvent(__is_creditor_subtotal_ready, company_id=company_id, creditor=creditor, value=0)
    assert event_200_balance()
    __sub_test_disbursement_finalize(token, chalice_gateway)
    assert event_zero_balance()


@pytest.mark.master_parallel_test
@mock.patch('chalicelib.moneyflow.disbursement.disbursement_finalization_class.get_user_email')
@mock.patch('chalicelib.moneyflow.disbursement.disbursement_finalization_class.get_company_email')
@mock.patch('chalicelib.moneyflow.disbursement.disbursement_finalization_class.mf_table_name_to_thread_var')
@mock.patch('chalicelib.moneyflow.disbursement.reporting.lambda_client.invoke')
@mock.patch('chalicelib.moneyflow.delegate_statement_report.get_email_for_entity')
def test_lambda_disbursement_finalization(
        mock_get_email_for_entity, mock_lambda_client_invoke, __mf_table_name_to_thread_var, __get_company_email,
        __get_user_email, chalice_gateway, rnd_token_id, rnd_company_v2, rnd_disbursement):
    __get_user_email.return_value = __get_company_email.return_value \
        = mock_get_email_for_entity.return_value = 'success@simulator.amazonses.com'
    # get company id and create a disbursement
    company_id = rnd_disbursement['owners'][0]['company_id']
    pending_payments = rnd_disbursement['payments']
    token = rnd_disbursement['token']
    user_id = rnd_disbursement['user_id']
    owner = create_ready_owner_disbursement(chalice_gateway, token, company_id, user_id, pending_payments)

    # prepare an event for lambda and run lambda
    disbursement = get_disbursement_record(company_id, user_id)
    message = {'disbursement': disbursement, 'user_id': user_id, 'company_id': company_id}
    DisbursementFinalization.lambda_disbursement_finalization(message, {})

    # assert that current_balance of disbursed owner is 0
    assert __is_owner_subtotal_ready(company_id, owner, 0)


@pytest.mark.master_parallel_test
@mock.patch('chalicelib.moneyflow.disbursement.disbursement_finalization_class.get_company_email')
@mock.patch('chalicelib.moneyflow.disbursement.disbursement_finalization_class.get_user_email')
@mock.patch('chalicelib.moneyflow.disbursement.disbursement_finalization_class.log_sns_disb_problem')
@mock.patch('chalicelib.moneyflow.disbursement.disbursement_finalization_class.logger.exception')
@mock.patch('chalicelib.moneyflow.disbursement.disbursement_finalization_class.add_pending_status')
@mock.patch('chalicelib.moneyflow.disbursement.disbursement_finalization_class.mf_table_name_to_thread_var')
@mock.patch('chalicelib.moneyflow.disbursement.disbursement_finalization_class.DisbursementFinalization.do_rollback')
def test_lambda_disbursement_finalization_exception_handling(
        __do_rollback, __mf_table_name_to_thread_var, __add_pending_status, __log_exception,
        __log_sns_disb_problem, __get_user_email, __get_company_email, chalice_gateway,
        rnd_token_id, rnd_company_v2, rnd_disbursement):
    __get_user_email.return_value = 'reports@mail.com'
    __get_company_email.return_value = 'mail@mail.com'
    __add_pending_status.side_effect = Exception('Test Exception Handling')
    # get company id and create a disbursement
    company_id = rnd_disbursement['owners'][0]['company_id']
    token = rnd_disbursement['token']
    user_id = rnd_disbursement['user_id']
    create_ready_owner_disbursement(chalice_gateway, token, company_id, user_id)

    # prepare an event for lambda and run lambda
    disbursement = get_disbursement_record(company_id, user_id)
    message = {'disbursement': disbursement, 'user_id': user_id, 'company_id': company_id}
    DisbursementFinalization.lambda_disbursement_finalization(message, {})
    __log_exception.assert_called()
    __do_rollback.assert_called()


@pytest.mark.master_parallel_test
@mock.patch('chalicelib.moneyflow.disbursement.disbursement_finalization_class.get_user_email')
@mock.patch('chalicelib.moneyflow.disbursement.disbursement_finalization_class.'
            'DisbursementFinalization.send_notification')
@mock.patch('chalicelib.moneyflow.disbursement.disbursement_finalization_class.lambda_client.invoke')
@mock.patch('chalicelib.moneyflow.disbursement.disbursement_finalization_class.'
            'DisbursementFinalization.log_disb_finalization_error')
def test_disb_rollback(mock_log_disb_finalization_error, mock_lambda_client_invoke, send_notification_,
                       get_user_email_, rnd_token_id, rnd_company_v2, rnd_disbursement, chalice_gateway):
    """
        1 create dataset
        2 mock functions to fail
        3 check that dataset is in original state
        4 check that no new data was left in database
    """
    get_user_email_.return_value = "reports@mail.com"
    send_notification_.side_effect = Exception()
    mock_log_disb_finalization_error.return_value = None
    mock_lambda_client_invoke.side_effect = lambda *args, **kwargs: call_lambda_rollback_disbursement(*args, **kwargs)

    company_id = rnd_disbursement['owners'][0]['company_id']
    token = rnd_disbursement['token']
    user_id = rnd_disbursement['user_id']
    __sub_test_create_disbursement(token, chalice_gateway)
    __sub_test_item_get_subtotal_disbursement_created(company_id, 'owner')
    __sub_test_update_disbursement(token, chalice_gateway, {"date": "2020-05-07"}, company_id, user_id)
    __sub_test_update_disbursement(token, chalice_gateway, {"payments": []}, company_id, user_id)
    __sub_test_prepare_owners_disbursement(token, chalice_gateway)
    owner = __sub_test_get_items_for_disbursement(token, chalice_gateway, "owners_balances")[0]
    __sub_test_update_disbursement(
        token, chalice_gateway, {"owners_to_disburse": [owner]}, company_id, user_id)
    __sub_test_update_disbursement(token, chalice_gateway, {"creditors_to_disburse": []}, company_id, user_id)
    __sub_test_update_disbursement(token, chalice_gateway, {"trust_account_reports": []}, company_id, user_id)
    __is_disbursement_ready(company_id, user_id, owners_len=1)

    disbursement = get_disbursement_record(company_id, user_id)
    disb_original_data = get_disb_data(disbursement, company_id)

    message = {
        'disbursement': disbursement,
        'user_id': user_id,
        'company_id': company_id
    }
    DisbursementFinalization(message).run()
    events_in_pending = WaiterEvent(__is_disbursement_done, company_id=company_id, attr='events_in_pending')
    assert events_in_pending()
    assert_disbursement_data(disb_original_data)
    mock_log_disb_finalization_error.assert_called_once()


@pytest.mark.master_parallel_test
@mock.patch('chalicelib.moneyflow.disbursement.disbursement_finalization_class.get_user_email')
@mock.patch('chalicelib.moneyflow.disbursement.disbursement_finalization_class.'
            'DisbursementFinalization.log_disb_finalization_error')
@mock.patch('chalicelib.moneyflow.disbursement.disbursement_finalization_class.mf_table_name_to_thread_var')
def test_lambda_rollback_disbursement(mock_mf_table_name_to_thread_var, mock_log_disb_finalization_error,
                                      mock_get_user_email, chalice_gateway, rnd_token_id,
                                      rnd_company_v2, rnd_disbursement):
    mock_get_user_email.return_value = "reports@mail.com"
    mock_log_disb_finalization_error.return_value = None
    mock_mf_table_name_to_thread_var.return_value = None

    # get company id and create a disbursement
    company_id = rnd_disbursement['owners'][0]['company_id']
    token = rnd_disbursement['token']
    user_id = rnd_disbursement['user_id']
    create_ready_owner_disbursement(chalice_gateway, token, company_id, user_id)

    # prepare an event for lambda and run lambda
    disbursement = get_disbursement_record(company_id, user_id)
    disb_original_data = get_disb_data(disbursement, company_id)
    message = {'rollback': disb_original_data, 'user_id': user_id, 'company_id': company_id}
    message = json.loads(json.dumps(message, default=parse_decimal))

    DisbursementFinalization.lambda_rollback_disbursement(message, {})
    # wait for rollback
    events_in_pending = WaiterEvent(__is_disbursement_done, company_id=company_id, attr='events_in_pending')
    assert events_in_pending()
    assert_disbursement_data(disb_original_data)
    mock_log_disb_finalization_error.assert_called_once()


def __get_balance(company_id, creditor_id, expected):
    return get_moneyflow_table().get_item(Key={
        'partkey': f'disb_subtotal_{company_id}',
        'sortkey': f'creditor_{creditor_id}'
    })['Item']['current_balance'] == expected


@pytest.mark.master_parallel_test
def test_lambda_creditors_disbursement_add_receipt(rnd_token_id, rnd_company_v2, rnd_disbursement, chalice_gateway, monkeypatch):
    # monkeypatch.setenv('DEBUG', 'true') # set for local debug
    company_id = rnd_disbursement['owners'][0]['company_id']
    token = rnd_disbursement['token']
    user_id = rnd_disbursement['user_id']
    __sub_test_create_disbursement(token, chalice_gateway)  # init disb
    __sub_test_prepare_creditors_disbursement(token, chalice_gateway)
    creditors = __sub_test_get_items_for_disbursement(token, chalice_gateway, "creditors_balances")
    creditor = [item for item in creditors if item['available'] == 200 and item['payment'] == 200][0]
    creditor_id = creditor['creditor_id']
    available = to_dec(creditor['available'], precision=Decimal('0.00'))
    amount = 415
    trans_type = 'Creditor'
    transaction = gen_transaction(
        ledger_from=creditor_id,
        ledger_to=creditor_id,
        amount=amount,
        transaction_type=trans_type,
        from_name='STL',
        date='2020-10-09'
    )
    request_create_transaction_class(chalice_gateway, token, transaction)
    balance_change = WaiterEvent(
        __get_balance, company_id=company_id, creditor_id=creditor_id, expected=Decimal(amount) + available)
    assert balance_change(), 'trigger not work'
    __sub_test_create_disbursement(token, chalice_gateway)  # init disb
    __sub_test_prepare_creditors_disbursement(token, chalice_gateway)
    creditor = [
        item for item in __sub_test_get_items_for_disbursement(token, chalice_gateway, "creditors_balances")
        if item.get('creditor_id') == creditor_id
    ][0]
    assert to_dec(creditor['available'], precision=Decimal('0.00')) == Decimal(amount) + available, (
            'Calculation is wrong')


@pytest.mark.master_parallel_test
def test_lambda_creditors_disbursement_add_withhold(
        rnd_token_id, rnd_company_v2, rnd_disbursement, chalice_gateway, monkeypatch):
    # monkeypatch.setenv('DEBUG', 'true') # set for local debug
    company_id = rnd_disbursement['owners'][0]['company_id']
    token = rnd_disbursement['token']
    user_id = rnd_disbursement['user_id']
    __sub_test_create_disbursement(token, chalice_gateway)  # init disb
    __sub_test_prepare_creditors_disbursement(token, chalice_gateway)
    creditors = __sub_test_get_items_for_disbursement(token, chalice_gateway, "creditors_balances")
    creditor = [item for item in creditors if item['available'] == 200 and item['payment'] == 200][0]
    creditor_id = creditor['creditor_id']
    creditor['withhold'] = 100
    update_expression, values, names = expression_preparation('withhold', **creditor)
    key = {
        'partkey': f'creditors_{company_id}',
        'sortkey': creditor['creditor_id']
    }
    get_moneyflow_table().update_item(
        Key=key,
        UpdateExpression=update_expression,
        ExpressionAttributeValues=values,
        ExpressionAttributeNames=names,
        ReturnValues='UPDATED_NEW'
    )
    __sub_test_create_disbursement(token, chalice_gateway)  # init disb
    __sub_test_prepare_creditors_disbursement(token, chalice_gateway)

    def has_creditor_in_creditors(creditor_):
        creditors_ = __sub_test_get_items_for_disbursement(token, chalice_gateway, "creditors_balances")
        res = [item for item in creditors_ if item.get('creditor_id') == creditor_]
        if len(res):
            return res[0]
        return False

    creditor_wait = WaiterEvent(has_creditor_in_creditors, creditor_=creditor_id)
    creditor = creditor_wait()
    assert to_dec(creditor['payment'], precision=Decimal('0.00')) == Decimal(100), 'Calculation is wrong'
