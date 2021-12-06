"""Convert PYMARC XML to Alma XML"""
import xml
import logging

logger = logging.getLogger(__name__)


def _get_subfields(subfields: dict) -> list:
    output = []
    for subfield, value in subfields.items():
        output.append({"code": subfield, "data": value})
    return output


def _get_variable_field(value, new_field):
    if "ind1" in value:

        new_field["inds"] = "".join([value["ind1"], value["ind2"]])
    new_field["subfields"] = []
    for row in value["subfields"]:
        new_field["subfields"].extend(_get_subfields(row))
    return new_field


def _get_fields(field):
    new_field = {}
    for tag, value in field.items():
        new_field["tag"] = tag
        if isinstance(value, str):
            new_field["subfields"] = [{"code": "_", "data": value}]
        else:
            new_field = _get_variable_field(value, new_field)
    return new_field


def to_alma_xml(**kwargs):
    """Convert pymarc MARC XML to Alma XML."""
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")

    for instance_uri in resources:
        marc_raw = task_instance.xcom_pull(
            key=instance_uri, task_ids="process_alma.marc_xml_to_s3"
        )
        pymarc_xml = xml.loads(marc_raw)
        record = {"standard": "MARC21", "type": "BIB", "fields": []}
        record["leader"] = pymarc_xml.get("leader")
        for field in pymarc_xml["fields"]:
            record["fields"].append(_get_fields(field))

        task_instance.xcom_push(key=instance_uri, value=record)
