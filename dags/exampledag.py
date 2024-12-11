from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List
from dataclasses import asdict
from decimal import Decimal
import json


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Au Pakin"},
    tags=["example"],
)
def migrate_company():

    BUSINESS_TYPE_MAPPING = {
        "บริการ": "service",
        "ค้าส่ง/ค้าปลีก": "trade",
        "การผลิต": "production"
    }

    @dataclass
    class Company:
        name_th: Optional[str]
        name_en: Optional[str]
        business_type: Optional[str]
        business_characteristics: Optional[str]
        past_income: Optional[float]
        income_year: Optional[float]

    @dataclass
    class CompanySecurities:
        id: Optional[int]
        security_id: Optional[int]
        name_th: Optional[str]
        name_en: Optional[str]
        business_type: Optional[str]
        product_description: Optional[str]
        juristic_id: Optional[str]
        phone_number: Optional[str]
        website_url: Optional[str]
        address_number: Optional[str]
        address_road: Optional[str]
        address_province: Optional[str]
        address_district: Optional[str]
        address_subdistrict: Optional[str]
        address_zipcode: Optional[str]
        revenue_amount: Optional[float]
        revenue_year: Optional[int]

    @dataclass
    class CompanyFiling:
        id: Optional[int]
        name_en: Optional[str]
        symbol: Optional[str]
        capital_detail_id: Optional[int]
        logo_url: Optional[str]
        authorized_capital: Optional[float]
        paid_up: Optional[float]
        listed_shared: Optional[int]
        preferred_shared: Optional[int]
        created_date: Optional[str]
        updated_date: Optional[str]
        state: Optional[str]

    @task
    def query_company_securities():
        """
        Queries all company securities and their associated revenue details.
        Returns the results as a list of tuples.
        """

        mysql_conn_id = "mysql_fundraising"
        query = """
            SELECT
                cs.id AS id,
                cs.security_id AS security_id,
                cs.name_th AS name_th,
                cs.name_en AS name_en,
                cs.business_type AS business_type,
                cs.product_description AS product_description,
                cs.juristic_id AS juristic_id,
                cs.phone_number AS phone_number,
                cs.website_url AS website_url,
                cs.address_number AS address_number,
                cs.address_road AS address_road,
                cs.address_province AS address_province,
                cs.address_district AS address_district,
                cs.address_subdistrict AS address_subdistrict,
                cs.address_zipcode AS address_zipcode,
                cr.revenue AS revenue_amount,
                cr.year AS revenue_year
            FROM
                company_securities cs
            LEFT JOIN
                company_revenue cr
            ON
                cs.companyRevenueId = cr.id
        """
        exchange_conn = MySqlHook(mysql_conn_id=mysql_conn_id)

        try:
            result = exchange_conn.get_records(query)

        finally:
            exchange_conn.get_conn().close()

        return result

    @task
    def map_to_company_securities(result) -> List[CompanySecurities]:
        mapped_data = [
            CompanySecurities(
                id=row[0],
                security_id=row[1],
                name_th=row[2],
                name_en=row[3],
                business_type=BUSINESS_TYPE_MAPPING.get(row[4], "unknown"),
                product_description=row[5],
                juristic_id=row[6],
                phone_number=row[7],
                website_url=row[8],
                address_number=row[9],
                address_road=row[10],
                address_province=row[11],
                address_district=row[12],
                address_subdistrict=row[13],
                address_zipcode=row[14],
                revenue_amount=row[15],
                revenue_year=row[16]
            ) for row in result
        ]

        serialized_data = [asdict(record) for record in mapped_data]

        return serialized_data

    @task
    def filter_invalid_companies(
        mapped_data: List[CompanySecurities]
    ) -> List[CompanySecurities]:
        filtered_data = []

        for record in mapped_data:
            if isinstance(record['id'], (int, float)) and record['security_id'] is not None:
                filtered_data.append(record)

        return filtered_data

    @task
    def query_company_filings(company_securities: List[CompanySecurities]):
        """
        Queries company filings for the filtered securities.
        """
        mysql_conn_id = "mysql_fundraising"
        results = []

        for security in company_securities:
            query = f"""
            SELECT
                cf.id,
                cf.name_en,
                cf.symbol,
                cf.captitalDetailId,
                ud.url AS logo_url,
                cd.authorized_capital,
                cd.paid_up,
                cd.listed_shared,
                cd.preferred_shared,
                cd.created_date,
                cd.updated_date,
                cf.state
            FROM
                company_filing cf
            LEFT JOIN
                upload_document ud ON cf.logoId = ud.id
            LEFT JOIN
                capital_detail cd ON cf.captitalDetailId = cd.id
            WHERE
                cf.companySecuritiesId = {security['id']}
                AND cf.state = 'before_crowd_opinion'
            """
            exchange_conn = MySqlHook(mysql_conn_id=mysql_conn_id)
            result = exchange_conn.get_records(query)
            results.extend(result)

        exchange_conn.get_conn().close()

        return results

    @task
    def map_to_company_filings(result) -> List[CompanyFiling]:
        """
        Maps the raw query result to a list of dictionaries, ensuring Decimal and datetime fields are serialized properly.
        """
        def convert_value(value):
            if isinstance(value, Decimal):
                return float(value)
            elif isinstance(value, datetime):
                return value.isoformat()
            return value

        mapped_data = [
            CompanyFiling(
                id=row[0],
                name_en=row[1],
                symbol=row[2],
                capital_detail_id=row[3],
                logo_url=row[4],
                authorized_capital=convert_value(row[5]),
                paid_up=convert_value(row[6]),
                listed_shared=row[7],
                preferred_shared=row[8],
                created_date=convert_value(row[9]),
                updated_date=convert_value(row[10]),
                state=row[11],
            )
            for row in result
        ]

        serialized_data = [asdict(record) for record in mapped_data]
        return serialized_data

    @task
    def print_data(data):
        """This task prints any data in a formatted way"""
        print(f"Printing {len(data)} records:")
        for record in data:
            print(json.dumps(record, indent=4, ensure_ascii=False))

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

    # insert_company_and_state_task = insert_company_and_state(
    #     map_to_company_filings_task, map_to_company_securities_task)

    query_company_securities_task >> map_to_company_securities_task
    filter_invalid_companies_task >> print_companies_task
    query_company_filings_task >> map_to_company_filings_task >> print_filings_task


migrate_company()
