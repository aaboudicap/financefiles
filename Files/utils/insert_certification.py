
import os
import pyodbc
import logging
import pandas as pd
from utils.excel_utils import clean_text, safe_date

SQL_CONN_STRING = os.environ.get("SqlConnectionString2")

def inserer_data_certifications(df_cert):

    inserted_count = 0
    ignored_count = 0
    error_count = 0

    required_columns = [
        "ggid", "certification", "theme",
        "date_passage", "statut_validite",
        "entite_niveau_1", "entite_niveau_2",
        "entite_niveau_3", "entite_niveau_4",
        "pu_code", "etablissement"
    ]
    missing = [c for c in required_columns if c not in df_cert.columns]
    if missing:
        raise ValueError(f"⚠️ Colonnes manquantes : {missing}")

    with pyodbc.connect(SQL_CONN_STRING) as conn:
        conn.autocommit = False
        cursor = conn.cursor()

        for index, row in df_cert.iterrows():
            try:
                if row.isnull().all():
                    ignored_count += 1
                    continue

                ggid = str(row["ggid"]).zfill(8)

                # ----------------------------
                # Vérifier si déjà présent
                # ----------------------------
                cursor.execute("""
                    SELECT 1 FROM dim_certification
                    WHERE ggid = ? AND libelle_certification = ? AND date_obtention = ?
                """,
                ggid,
                clean_text(row["certification"]),
                safe_date(row["date_passage"])
                )

                if cursor.fetchone():
                    ignored_count += 1
                    logging.info(f"⏭️ Certification déjà existante pour {ggid}")
                    continue

                # ----------------------------
                # Insertion dans dim_certification
                # ----------------------------
                cursor.execute("""
                    INSERT INTO dim_certification (
                        ggid,
                        libelle_certification,
                        domaine,
                        date_obtention,
                        statut_validite,
                        entite_niveau_1,
                        entite_niveau_2,
                        entite_niveau_3,
                        entite_niveau_4,
                        pu_code,
                        etablissement
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ggid,
                clean_text(row["certification"]),
                clean_text(row["theme"]),
                safe_date(row["date_passage"]),
                clean_text(row["statut_validite"]),
                row.get("entite_niveau_1"),
                row.get("entite_niveau_2"),
                row.get("entite_niveau_3"),
                row.get("entite_niveau_4"),
                row.get("pu_code"),
                row.get("etablissement")
                )

                inserted_count += 1
                logging.info(f"✅ Certification insérée pour {ggid}")

            except Exception as e:
                conn.rollback()
                error_count += 1
                logging.error(f"❌ Erreur ligne {index+2} : {e}")
                raise

        conn.commit()

    logging.info(f"📊 Résultat: {inserted_count} insérées, {ignored_count} ignorées, {error_count} erreurs.")
    return inserted_count, ignored_count, error_count
