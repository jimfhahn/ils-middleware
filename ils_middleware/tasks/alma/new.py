"""New XML to Alma"""
import xml

from ils_middleware.tasks.alma.post import almaRequest


def NewMARCtoAlma(**kwargs):
    """Creates a new record in Alma and returns the new MMSID"""
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(
        key="new_resources", task_ids="process_alma.new"
    )

    for resource_uri in resources:
        marc_record = task_instance.xcom_pull(
            key=resource_uri, task_ids="process_alma.mod_to_alma_xml"
        )

        payload = {"bib": marc_record}

        task_instance.xcom_push(
            key=resource_uri,
            value=almaRequest(
                **kwargs,
                data=xml.dumps(payload),
                endpoint="catalog/bib",
                filter=lambda response: response.xml().get("@key"),
            ),
        )
