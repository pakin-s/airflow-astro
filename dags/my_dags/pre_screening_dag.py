from airflow.decorators import dag
from datetime import datetime
from tasks.pre_screening_tasks import (
    query_securities,
    map_to_securities,
    filter_invalid_companies,
    query_filings,
    map_to_filings,
    print_data,
    filter_securities_by_filing,
    insert_company_and_current_state,
    insert_company_information,
    insert_capital_details,
)


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Au Pakin"},
    tags=["example"],
)
def migrate_company():

    query_securities_task = query_securities()

    map_to_securities_task = map_to_securities(
        query_securities_task)

    filter_invalid_securities_task = filter_invalid_companies(
        map_to_securities_task)

    query_filings_task = query_filings(
        filter_invalid_securities_task)

    map_to_filings_task = map_to_filings(
        query_filings_task)

    filter_securities_by_filings_task = filter_securities_by_filing(
        map_to_securities_task, map_to_filings_task)

    filter_null_securities = filter_invalid_companies(
        filter_securities_by_filings_task)

    insert_company_and_current_state_task = insert_company_and_current_state(
        map_to_filings_task, filter_null_securities)

    insert_capital_details_task = insert_capital_details(map_to_filings_task)

    insert_company_information_task = insert_company_information(
        map_to_filings_task, filter_null_securities, insert_capital_details_task)

    query_securities_task >> map_to_securities_task
    filter_invalid_securities_task
    query_filings_task >> map_to_filings_task
    filter_securities_by_filings_task >> filter_null_securities
    insert_company_and_current_state_task >> insert_capital_details_task
    insert_company_information_task


migrate_company()
