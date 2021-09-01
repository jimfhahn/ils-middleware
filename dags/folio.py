import logging

# from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator


def map_to_folio(urls: list):
    logging.info(f"Starting with {len(urls)} from Sinopia")
    for url in urls:
        logging.info(f"Sinopia {url} retrives Document")
    logging.info(f"Finished with {len(urls)} from Sinopi")
    logging.info("Magic Mapping to FOLIO happens here")
    return "folio_send"
