import json
import logging
import os
from uuid import uuid4
from typing import Dict, List

import requests
from kafka import KafkaConsumer, KafkaProducer
from requests.auth import HTTPBasicAuth
# import shared

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)


TAXAMORPH_ENDPOINT = 'https://merry-malamute-bold.ngrok-free.app/infer'


def start_kafka() -> None:
    """
    Start a kafka listener and process the messages by unpacking the image.
    When done it will republish the object, so it can be validated and stored by the processing service
    """
    consumer = KafkaConsumer(
        os.environ.get("KAFKA_CONSUMER_TOPIC"),
        group_id=os.environ.get("KAFKA_CONSUMER_GROUP"),
        bootstrap_servers=[os.environ.get("KAFKA_CONSUMER_HOST")],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=True,
    )
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_PRODUCER_HOST")],
        value_serializer=lambda m: json.dumps(m).encode("utf-8"),
    )
    for msg in consumer:
        try:
            logging.info("Received message: " + str(msg.value))
            json_value = msg.value
            shared.mark_job_as_running(json_value.get("jobId"))
            specimen_data = json_value.get("object")
            result = run_api_call(specimen_data)
            mas_job_record = map_to_annotation_event(
                specimen_data, result, json_value.get("jobId")
            )
            publish_annotation_event(mas_job_record, producer)
        except Exception as e:
            logging.exception(e)


def map_to_annotation_event(
    specimen_data: Dict, results: List[Dict[str, str]], job_id: str
) -> Dict:
    """
    Map the result of the API call to an annotation
    :param specimen_data: The JSON value of the Digital Specimen
    :param results: A list of results that contain the queryString and the BOLD EU process identifier
    :param job_id: The job ID of the MAS
    :return: Returns a formatted annotation Record which includes the Job ID
    """
    timestamp = shared.timestamp_now()
    if results is None:
        annotations = list()
    else:
        annotations = list(
            map(
                lambda result: map_result_to_annotation(
                    specimen_data, result, timestamp
                ),
                results,
            )
        )
    annotation_event = {"jobId": job_id, "annotations": annotations}
    return annotation_event


def map_result_to_annotation(
    specimen_data: Dict, result: Dict[str, str], timestamp: str
) -> Dict:
    """
    Map the result of the API call to an annotation
    :param specimen_data: The original specimen data
    :param result: The result from BOLD EU, contains the Bold EU processid and the queryString
    :param timestamp: A formatted timestamp of the current time
    :return: Returns a formatted annotation
    """
    ods_agent = shared.get_agent()
    oa_value = shared.map_to_entity_relationship(
        "hasBOLDEUProcessID",
        result["processid"],
        f"https://boldsystems.eu/record/{result['processid']}",
        timestamp,
        ods_agent,
    )
    oa_selector = shared.build_class_selector(shared.ER_PATH)
    return shared.map_to_annotation(
        ods_agent,
        timestamp,
        oa_value,
        oa_selector,
        specimen_data[shared.ODS_ID],
        specimen_data[shared.ODS_TYPE],
        result["queryString"],
    )


def publish_annotation_event(annotation_event: Dict, producer: KafkaProducer) -> None:
    """
    Send the annotation to the Kafka topic
    :param annotation_event: The formatted annotationRecord
    :param producer: The initiated Kafka producer
    :return: Will not return anything
    """
    logging.info("Publishing annotation: " + str(annotation_event))
    producer.send(os.environ.get("KAFKA_PRODUCER_TOPIC"), annotation_event)


def run_api_call(digital_specimen: Dict) -> List[Dict[str, str]]:
    """
    Calls BOLD EU API based on the available identifiers.
    It is possible that one Digital Specimen has multiple BOLD records.
    :param specimen_data: The JSON data of the Digital Specimen
    :return:  A list of results that contain the queryString and the BOLD EU process ids
    """

    print(digital_specimen)

    data = {
        "jobId": "20.5000.1025/AAA-111-BBB",
        "object": {
            "digitalSpecimen": digital_specimen
        },
        "batchingRequested": False
    }    

    # Send the POST request to the server
    response = requests.post(TAXAMORPH_ENDPOINT, json=data)
    print(response)
    # result = response.json()
    # print("Response from server:")
    # print(result)


def run_local(specimen_id: str) -> None:
    """
    Runs script locally. Demonstrates using a specimen target
    :param specimen_id: A specimen ID from DiSSCo Sandbox Environment https://sandbox.dissco.tech/search
    Example: SANDBOX/KMP-FZ6-S2K
    :return: Return nothing but will log the result
    """
    digital_specimen = (
        requests.get(
            f"https://sandbox.dissco.tech/api/digital-specimen/v1/{specimen_id}"
        )
        .json()
        .get("data")
        .get("attributes")
    )

    print(digital_specimen)

    result = run_api_call(digital_specimen)
    # mas_job_record = map_to_annotation_event(specimen_data, result, str(uuid4()))
    # logging.info("Created annotations: " + json.dumps(mas_job_record, indent=2))


if __name__ == "__main__":
    run_local('SANDBOX/241-QN4-94Y')
    # run_local("https://dev.dissco.tech/api/digital-specimen/v1/TEST/MJG-GTC-5C2")