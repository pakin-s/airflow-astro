from airflow.decorators import dag
from datetime import datetime
from tasks.pre_screening_tasks import (
    query_company_securities,
    map_to_company_securities,
    filter_invalid_companies,
    query_company_filings,
    map_to_company_filings,
    print_data,
    filter_securities_from_filing
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

    query_company_securities_task = query_company_securities()

    map_to_company_securities_task = map_to_company_securities(
        query_company_securities_task)

    filter_invalid_companies_task = filter_invalid_companies(
        map_to_company_securities_task)

    print_companies_task = print_data(filter_invalid_companies_task)

    query_company_filings_task = query_company_filings(
        filter_invalid_companies_task)

    map_to_company_filings_task = map_to_company_filings(
        query_company_filings_task)

    print_filings_task = print_data(map_to_company_filings_task)

    filter_securities_from_filing_task = filter_securities_from_filing(
        map_to_company_securities_task, map_to_company_filings_task)

    filter_xxx = filter_invalid_companies(filter_securities_from_filing_task)

    print_xxx = print_data(filter_xxx)

    # insert_company_and_state_task = insert_company_and_state(
    #     map_to_company_filings_task, map_to_company_securities_task)

    query_company_securities_task >> map_to_company_securities_task
    filter_invalid_companies_task >> print_companies_task
    query_company_filings_task >> map_to_company_filings_task >> print_filings_task
    filter_securities_from_filing_task >> filter_xxx >> print_xxx


migrate_company()
