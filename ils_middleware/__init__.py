import logging
import os
import sys

from honeybadger import honeybadger


LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)

logger = logging.getLogger(__name__)
logger.setLevel(LOGLEVEL)
logger.addHandler(logging.StreamHandler(sys.stdout))


HONEYBADGER_API_KEY = os.environ.get("HONEYBADGER_API_KEY")
logger.info(f"HONEYBADGER_API_KEY={HONEYBADGER_API_KEY}")

honeybadger.configure(
    api_key=HONEYBADGER_API_KEY, environment=os.environ.get("AIRFLOW_VAR_SINOPIA_ENV")
)
logging.getLogger("honeybadger").addHandler(logging.StreamHandler())
