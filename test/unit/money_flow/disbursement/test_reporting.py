from test.test_utils.fixtures import *

import pytest
import mock

from chalicelib.moneyflow.disbursement.reporting import DisbursementReporting


@pytest.mark.local_db_test
@mock.patch('chalicelib.moneyflow.disbursement.reporting.make_statement_reports')
@mock.patch('chalicelib.moneyflow.disbursement.reporting.make_user_reports')
@mock.patch('chalicelib.moneyflow.disbursement.reporting.generate_report_bpay')
@mock.patch('chalicelib.moneyflow.disbursement.reporting.generate_report_eft')
def test_disb_reporting(_generate_report_eft, _generate_report_bpay, _make_user_reports, _make_statement_reports,
                        rnd_company_v2, rnd_token_id, rnd_processed_disb):
    company_id = rnd_company_v2['company']['id_']
    disb_id = rnd_processed_disb['id_']
    DisbursementReporting(company_id, disb_id).do_reporting()

    _generate_report_eft.assert_called_once()
    _generate_report_bpay.assert_called_once()
    _make_user_reports.assert_called_once()
    _make_statement_reports.assert_called_once()
