from airflow.decorators import dag
from datetime import datetime
from tasks.migrate_company_data_task import (
    query_all_company_securities,
    map_to_company_securities_list,
    filter_invalid_companies,
    print_data,
    map_to_company_list,
    map_to_company_information_list
)

@dag(
    dag_id="migrate_company_data",
    start_date=datetime(2024, 11, 21),
    schedule_interval=None,
    catchup=False,
    default_args={"owner": "doctoey"}
)

def migrate_company_data():

    query_all_company_securities_task = query_all_company_securities()

    map_to_company_securities_list_task = map_to_company_securities_list(query_all_company_securities_task)

    filter_invalid_companies_task = filter_invalid_companies(map_to_company_securities_list_task)

    map_company_task = map_to_company_list(filter_invalid_companies_task)
    map_to_company_information_list_task = map_to_company_information_list(filter_invalid_companies_task)

    # print_task = print_data(filter_invalid_companies_task)
    print_company_task = print_data(map_company_task)
    print_company_information_task = print_data(map_to_company_information_list_task)

    query_all_company_securities_task >> map_to_company_securities_list_task >> filter_invalid_companies_task
    filter_invalid_companies_task >> map_company_task >> print_company_task
    filter_invalid_companies_task >> map_to_company_information_list_task >> print_company_information_task


migrate_company_data()