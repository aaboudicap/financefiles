import logging
from handlers.transform_whz_activites import nettoyer_fichier_activites
from utils.insert_activites import inserer_data_disponibilites
from handlers.transform_prh_certifications import nettoyer_prh_certifications
from utils.insert_certification import inserer_data_certifications


def archiver_fichier(blob_client, filename, container_client):
    archive_blob_name = f"archive/{filename}"
    archive_blob_client = container_client.get_blob_client(archive_blob_name)

    # Télécharger le contenu du blob original
    data = blob_client.download_blob().readall()

    # Uploader dans le dossier archive
    archive_blob_client.upload_blob(data, overwrite=True)
    logging.info(f"✅ Archivé sous : {archive_blob_name}")

    # Supprimer le fichier original
    blob_client.delete_blob()
    logging.info(f"🗑️ Supprimé : {filename}")

def route_file(filename, blob_client, container_client):
    name = filename.lower()
    logging.info(f"🧭 Routing du fichier : {filename}")

    if name.startswith("whz"):
        logging.info("🔧 Handler WHZ sélectionné")
        try:   
            df= nettoyer_fichier_activites(blob_client)
            inserer_data_disponibilites(df)
            logging.info("✅ Nettoyage du fichier whoz activité terminé avec succés")
            archiver_fichier(blob_client, filename, container_client)
            logging.info(f"✅ Archivage terminé avec succès pour le fichier : {filename}")
        except Exception as e:
            logging.error(f"❌ Le traitement du fichier {filename} a échoué : {e}")
        return

    elif name.startswith("prh"):
        logging.info("🔧 Handler PRH sélectionné")
        try:   
            df= nettoyer_prh_certifications(blob_client)
            inserer_data_certifications(df)
            logging.info("✅ Nettoyage du fichier PEPSRH terminé avec succés")
            archiver_fichier(blob_client, filename, container_client)
            logging.info(f"✅ Archivage terminé avec succès pour le fichier : {filename}")
        except Exception as e:
            logging.error(f"❌ Le traitement du fichier {filename} a échoué : {e}")
        return    

    else:
        logging.warning(f"❗ Aucun handler trouvé pour le fichier : {filename}")