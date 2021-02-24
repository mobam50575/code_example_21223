import json
import os
import random
import uuid

import boto3
import jwt
import pytest

from xml.etree import cElementTree as ElementTree

from boto3.dynamodb.conditions import Key
from n_utils.utils import query_items_paged, get_moneyflow_table
from n_utils.keys_structure import transactions_pk

from chalicelib.moneyflow.delegate_report_data import save_entity_xml_on_s3, save_entities_xml_on_s3
from test.test_utils.fixtures import chalice_gateway, rnd_company_v2, rnd_token_id, create_tenant, create_owner, \
    create_creditor, random_digit, create_any_transaction, audit_sequence_mask

s3_client = boto3.client('s3')

year_month = "2020-03"


@pytest.mark.local_db_test
def test_xml_report_data_by_id(request, rnd_token_id):
    token = json.loads(rnd_token_id['body'])['token']
    decoded_token = jwt.decode(token, '', algorithms=['RS256'], verify=False)
    user_id = decoded_token['sub']
    company_id = decoded_token['custom:company_id']
    company = {'company': {'id_': company_id}}

    entities_list = []
    number_of_tenants = random.randint(3, 7)
    for i in range(number_of_tenants):
        entities_list.append(create_tenant(company, 'test_owner', 'test_property'))
    number_of_owners = random.randint(3, 7)
    for i in range(number_of_owners):
        entities_list.append(create_owner(company, str(uuid.uuid4()), 'test_user', random_digit()))
    number_of_creditors = random.randint(3, 7)
    for i in range(number_of_creditors):
        entities_list.append(create_creditor(company, str(uuid.uuid4()), 'test_user', random_digit()))

    chosen_entity = entities_list[random.randint(0, len(entities_list) - 1)]

    number_of_transactions = random.randint(3, 7)
    audit_partkey = "101"
    audit_sequence = "00505"
    for i in range(number_of_transactions):
        create_any_transaction({"id_": chosen_entity["id_"]},
                               {"id_": entities_list[random.randint(0, len(entities_list) - 1)]["id_"]},
                               user_id, company, audit_partkey, audit_sequence_mask(audit_sequence, i), type_='test')

    records_type = f"{chosen_entity['record_type']}"
    query_params = {
        "entity_type": records_type,
        "date_start": f"{year_month}-01",
        "date_end": ""
    }
    file_key = save_entity_xml_on_s3(company_id, query_params, chosen_entity['id_'])

    def delete_at_final():
        boto3.resource('s3').Object(os.environ["REPORTS_BUCKET"], file_key).delete()
    request.addfinalizer(delete_at_final)

    xml_file = boto3.resource('s3').Object(os.environ["REPORTS_BUCKET"], file_key).get()['Body'].read()
    xml_root = ElementTree.XML(xml_file)
    assert bool(xml_root.find('company')), "Company details are missing"
    assert len([i for i in xml_root.find(records_type).iter('item')]) == 1
    for entity in xml_root.find(records_type).iter('item'):
        assert entity.find("id").text == chosen_entity["id_"], f"Entity with id_ {entity.find('id').text} is missing"
    assert len([i for i in xml_root.find('transactions').iter('item')]) == number_of_transactions, (
        "Wrong number of transaction")


@pytest.mark.local_db_test
def test_xml_report_data_transaction_by_period_with_tr(request, rnd_token_id):
    token = json.loads(rnd_token_id['body'])['token']
    decoded_token = jwt.decode(token, '', algorithms=['RS256'], verify=False)
    user_id = decoded_token['sub']
    company_id = decoded_token['custom:company_id']
    company = {'company': {'id_': company_id}}

    owners = []
    number_of_owners = random.randint(3, 7)
    for i in range(number_of_owners):
        owners.append(create_owner(company, str(uuid.uuid4()), 'test_user', random_digit()))
    creditors = []
    number_of_creditors = random.randint(3, 7)
    for i in range(number_of_creditors):
        creditors.append(create_creditor(company, str(uuid.uuid4()), 'test_user', random_digit()))

    number_of_transactions = random.randint(3, 7)
    audit_partkey = "101"
    audit_sequence = "00505"
    for i in range(number_of_transactions):
        create_any_transaction(owners[random.randint(0, len(owners) - 1)],
                               creditors[random.randint(0, len(creditors) - 1)],
                               user_id, company, audit_partkey, audit_sequence_mask(audit_sequence, i), type_='test')

    query_params = {"entities_types": ["owners"], "date_start": f"{year_month}-01"}
    file_key = save_entities_xml_on_s3(company_id, query_params)

    def delete_at_final():
        boto3.resource('s3').Object(os.environ["REPORTS_BUCKET"], file_key).delete()
    request.addfinalizer(delete_at_final)

    xml_file = boto3.resource('s3').Object(os.environ["REPORTS_BUCKET"], file_key).get()['Body'].read()
    xml_root = ElementTree.XML(xml_file)
    assert bool(xml_root.find('company')), "Company details are missing"
    assert len([i for i in xml_root.find('owners').iter('item')]) == number_of_owners, "Wrong number of owners"
    assert len([i for i in xml_root.find('transactions').iter('item')]) == number_of_transactions


@pytest.mark.local_db_test
def test_xml_report_data_transaction_by_period_without_tr(request, rnd_token_id):
    token = json.loads(rnd_token_id['body'])['token']
    decoded_token = jwt.decode(token, '', algorithms=['RS256'], verify=False)
    user_id = decoded_token['sub']
    company_id = decoded_token['custom:company_id']
    company = {'company': {'id_': company_id}}

    owners = []
    number_of_owners = random.randint(3, 7)
    for i in range(number_of_owners):
        owners.append(create_owner(company, str(uuid.uuid4()), 'test_user', random_digit()))
    creditors = []
    number_of_creditors = random.randint(3, 7)
    for i in range(number_of_creditors):
        creditors.append(create_creditor(company, str(uuid.uuid4()), 'test_user', random_digit()))

    number_of_transactions = random.randint(3, 7)
    audit_partkey = "101"
    audit_sequence = "00505"
    for i in range(number_of_transactions):
        create_any_transaction(owners[random.randint(0, len(owners) - 1)],
                               creditors[random.randint(0, len(creditors) - 1)],
                               user_id, company, audit_partkey, audit_sequence_mask(audit_sequence, i), type_='test')

    transactions_in_db = query_items_paged(
        Key('partkey').eq(transactions_pk.format(company_id=company_id, year_month=year_month)),
        table=get_moneyflow_table
    )
    assert len(transactions_in_db) == number_of_transactions, "There are no transactions for test"

    query_params = {"entities_types": ["owners"], "date_start": "2019-01-01", "date_end": "2020-02-01"}
    file_key = save_entities_xml_on_s3(company_id, query_params)

    def delete_at_final():
        boto3.resource('s3').Object(os.environ["REPORTS_BUCKET"], file_key).delete()
    request.addfinalizer(delete_at_final)

    xml_file = boto3.resource('s3').Object(os.environ["REPORTS_BUCKET"], file_key).get()['Body'].read()
    xml_root = ElementTree.XML(xml_file)
    assert bool(xml_root.find('company')), "Company details are missing"
    assert len([i for i in xml_root.find('owners').iter('item')]) == number_of_owners, "Wrong number of owners"
    assert len([i for i in xml_root.find('transactions').iter('item')]) == 0, "Unexpected transactions"


@pytest.mark.local_db_test
def test_xml_report_data_tenants(request, rnd_token_id):
    token = json.loads(rnd_token_id['body'])['token']
    decoded_token = jwt.decode(token, '', algorithms=['RS256'], verify=False)
    company_id = decoded_token['custom:company_id']
    company = {'company': {'id_': company_id}}

    number_of_tenants = random.randint(3, 7)
    for i in range(number_of_tenants):
        create_tenant(company, 'test_owner', 'test_property')

    query_params = {"entities_types": ["tenants"]}
    file_key = save_entities_xml_on_s3(company_id, query_params)

    def delete_at_final():
        boto3.resource('s3').Object(os.environ["REPORTS_BUCKET"], file_key).delete()
    request.addfinalizer(delete_at_final)

    xml_file = boto3.resource('s3').Object(os.environ["REPORTS_BUCKET"], file_key).get()['Body'].read()
    xml_root = ElementTree.XML(xml_file)
    assert bool(xml_root.find('company')), "Company details are missing"
    assert len([i for i in xml_root.find('tenants').iter('item')]) == number_of_tenants, "Wrong number of owners"
