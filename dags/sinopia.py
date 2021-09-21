"""Sinopia Operators and Functions for Institutional DAGs."""
import logging
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.sensors.bash_sensor import BashSensor


def UpdateIdentifier(**kwargs) -> PythonOperator:
    """Add Identifier to Sinopia."""
    return PythonOperator(
        task_id="sinopia-id-update",
        python_callable=sinopia_update,
        op_kwargs={"urls": kwargs.get("urls", []), "identifer": kwargs.get("id")},
    )


def sinopia_update(**kwargs):
    """Stub for updating Sinopia RDF resource with identifier."""
    urls = kwargs.get("urls")
    identifier = kwargs.get("identifier")
    logging.info(f"Starts updating Sinopia {len(urls)} resources")
    for url in urls:
        logging.info(f"Would PUT to Sinopia API {identifier} for {url}")
    logging.info(f"Ends updating Sinopia {identifier}")


def GitRdf2Marc() -> BashSensor:
    """Clones https://github.com/ld4p/rdf2marc repository if it doesn't exist."""
    logging.info("Checks and clones rdf2marc")
    sensor_rdf2marc_cmd = """
    if [ ! -d rdf2marc ]; then
        echo "rdf2marc does not exist. Cloning now"
        git clone https://github.com/ld4p/rdf2marc --depth=1
        exit 0
    fi
    cd rdf2marc
    git fetch
    local_hash = `git rev-parse main`
    remote_hash = `git rev-parse origin/main`
    if [ $local_hash != $remote_hash]
    then
        echo "Pulling in latest changes"
        git pull origin main
        exit 0
    else
        echo "Everything is updated with origin/main"
        exit 1
    fi
    """
    return BashSensor(
        task_id="git_rdf2marc",
        bash_command=sensor_rdf2marc_cmd,
        poke_interval=60,
        timeout=60 * 50,
    )


def Rdf2Marc(**kwargs) -> BashOperator:
    """Run rdf2marc on a BF Instance URL."""
    instance_url = kwargs.get("instance_url")
    if instance_url is None:
        raise ValueError("Missing Instance URL")
    return BashOperator(
        task_id="rdf2marc", bash_command=f"./exe/rdf2marc {instance_url}"
    )
