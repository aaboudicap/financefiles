import logging
import azure.functions as func
import os
from azure.storage.blob import BlobServiceClient
from utils.router import route_file

app = func.FunctionApp()

AZURE_STORAGE_CONNECTION_STRING = os.environ["stordatasynergyprod01_STORAGE"]
CONTAINER_NAME = "finance"
ARCHIVE_PREFIX = "archive/"

@app.function_name(name="TriggerFinance")
@app.timer_trigger(schedule="0 */2 * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 

def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
 
    # Lister tous les blobs du conteneur
    blobs = container_client.list_blobs()
 
    for blob in blobs:
        filename= blob.name
        if not filename.endswith(".xlsx") or filename.startswith(ARCHIVE_PREFIX):
            continue

        logging.info(f"📄 Fichier détecté : {filename}")
        blob_client = container_client.get_blob_client(blob)
        logging.info("⏭️ envoie du fichier au routeur pour traitement ....")
        route_file(filename, blob_client, container_client)

    logging.info(" Tous les fichiers ont été traité par la fonction local")
