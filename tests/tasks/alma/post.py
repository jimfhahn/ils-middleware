"""Test POST API function for Alma API"""
import logging
import requests  # type: ignore

from airflow.hooks.base_hook import BaseHook

logger = logging.getLogger(__name__)

"""Write test for new.py use examples from /tests/tasks/symphony/test_symphony_request.py
"""