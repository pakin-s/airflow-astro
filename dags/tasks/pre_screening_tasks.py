from airflow.providers.mysql.hooks.mysql import MySqlHook
from typing import List
from models.pre_screening_models import CompanySecurities, CompanyFiling, BUSINESS_TYPE_MAPPING
from airflow.decorators import task
from decimal import Decimal
from datetime import datetime
from dataclasses import asdict
import json


@task
def query_securities():
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
def map_to_securities(result) -> List[CompanySecurities]:
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

    return [asdict(record) for record in mapped_data]


@task
def filter_invalid_companies(mapped_data: List[CompanySecurities]) -> List[CompanySecurities]:
    filtered_data = [record for record in mapped_data if isinstance(
        record['id'], (int, float)) and record['security_id'] is not None]
    return filtered_data


@task
def query_filings(company_securities: List[CompanySecurities]):
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
            AND cf.symbol IS NOT NULL
        """
        exchange_conn = MySqlHook(mysql_conn_id=mysql_conn_id)
        result = exchange_conn.get_records(query)
        results.extend(result)

    exchange_conn.get_conn().close()

    return results


@task
def map_to_filings(result) -> List[CompanyFiling]:
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

    return [asdict(record) for record in mapped_data]


@task
def filter_securities_by_filing(securities: List[dict], filings: List[dict]) -> List[dict]:
    filing_names = {filing.get("name_en")
                    for filing in filings if filing.get("name_en")}

    filtered_securities = [
        security for security in securities if security.get("name_en") in filing_names
    ]

    return filtered_securities


@task
def insert_company_and_current_state(filings: List[dict], companies: List[dict]):
    mysql_conn_id = "mysql_backoffice"

    insert_company_currnet_state_query = """
        INSERT INTO company_current_state (
            current_state, submitted_date, approved_date, submitted_draft_date
        )
        VALUES (%s, %s, %s, %s)
    """

    insert_company_query = """
        INSERT INTO company (
            name_th, name_en, symbol, logo, establishment_date,
            business_type, business_characteristics, company_type,
            past_income, income_year, company_current_state_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    update_company_current_state_fk_query = """
            UPDATE company_current_state
            SET company_id = %s
            WHERE id = %s
    """

    revamp_conn = MySqlHook(mysql_conn_id=mysql_conn_id)
    conn = revamp_conn.get_conn()

    try:
        with conn.cursor() as cursor:
            current_time = datetime.now()

            inserted_current_state_ids = []
            for filing, company in zip(filings, companies):
                cursor.execute(insert_company_currnet_state_query, (
                    'pre_screening',  # this is hardcoding cuz we focus on only pre-screening, and I lazy to map
                    current_time,
                    current_time,
                    current_time,
                ))

                current_state_id = cursor.lastrowid
                inserted_current_state_ids.append(current_state_id)

                cursor.execute(insert_company_query, (
                    company['name_th'],
                    company['name_en'],
                    filing['symbol'],
                    filing['logo_url'],
                    filing['created_date'],
                    company['business_type'],
                    company['product_description'],
                    'sme',  # same as above here, if have time will refactor
                    company['revenue_amount'],
                    company['revenue_year'],
                    current_state_id,
                ))

                cursor.execute(update_company_current_state_fk_query, (
                    cursor.lastrowid,
                    current_state_id,
                ))

            conn.commit()
            print(f"Inserted {len(filings)} company current states and {
                  len(companies)} companies.")

    except Exception as e:
        print(f"Error inserting companies and company current states: {e}")
        conn.rollback()
    finally:
        conn.close()


@task
def insert_capital_details(filings: List[dict]) -> List[int]:
    mysql_conn_id = "mysql_backoffice"

    insert_capital_detail_query = """
        INSERT INTO capital_detail (
                authorized_capital, paid_up_capital, listed_shared,
                preferred_shared, capital_created_date, capital_updated_date
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    revamp_conn = MySqlHook(mysql_conn_id=mysql_conn_id)
    conn = revamp_conn.get_conn()

    try:
        with conn.cursor() as cursor:
            inserted_capital_detail_ids = []
            for filing in filings:
                cursor.execute(insert_capital_detail_query, (
                    filing['authorized_capital'],
                    filing['paid_up'],
                    filing['listed_shared'],
                    filing['preferred_shared'],
                    filing['created_date'],
                    filing['updated_date'],
                ))

                capital_detail_id = cursor.lastrowid
                inserted_capital_detail_ids.append(capital_detail_id)
            conn.commit()
            print(f"Inserted {len(filings)} capital detail.")

    except Exception as e:
        print(f"Error inserting capital detail: {e}")
        conn.rollback()
    finally:
        conn.close()
        return inserted_capital_detail_ids


@task
def insert_company_information(filings: List[dict], companies: List[dict], capital_ids: List[int]):
    mysql_conn_id = "mysql_backoffice"

    insert_company_information_query = """
        INSERT INTO company_information (
            name_th, name_en, logo, juristic_id, phone_number, website_url,
            address_number, road, province, district, sub_district,
            postal_code, business_type, capital_detail_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    with MySqlHook(mysql_conn_id=mysql_conn_id).get_conn() as conn:
        with conn.cursor() as cursor:
            for filing, company, capital_id in zip(filings, companies, capital_ids):
                company_info = (
                    company['name_th'],
                    company['name_en'],
                    filing['logo_url'],
                    company['juristic_id'],
                    company['phone_number'],
                    company['website_url'],
                    company['address_number'],
                    company['address_road'],
                    company['address_province'],
                    company['address_district'],
                    company['address_subdistrict'],
                    company['address_zipcode'],
                    company['business_type'],
                    capital_id,
                )

                cursor.execute(insert_company_information_query, company_info)

            conn.commit()
            print(f"Inserted {len(filings)} company information records.")


@task
def print_data(data):
    print(f"Printing {len(data)} records:")
    for record in data:
        print(json.dumps(record, indent=4, ensure_ascii=False))
