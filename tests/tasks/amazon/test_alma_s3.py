import logging
from urllib.parse import urlparse
import os
from os import path

from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pymarc import MARCReader
from pymarc import XMLWriter

logger = logging.getLogger(__name__)


"""Write a test for amazon alma s3 task"""