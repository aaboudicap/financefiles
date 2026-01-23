import os
import pyodbc
import logging
import pandas as pd
from utils.excel_utils import safe_date

SQL_CONN_STRING = os.environ.get("DatabaseConnection")


def inserer_data_disponibilites(df):
    required_columns = ['ggid', 'nom', 'prenom', 'grade','manager', 'mois', 'gross_capacity', 'billable', 'available','training','arve_cis_fr', 'semaine']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"⚠️ Colonnes manquantes dans le fichier : {missing_columns}")

    with pyodbc.connect(SQL_CONN_STRING) as conn:
        conn.autocommit = False  
        cursor = conn.cursor()

        try:
            for index, row in df.iterrows():
                if row.isnull().all():
                    logging.info(f"⏭️ Ligne {index} ignorée car vide.")
                    continue

                ggid = str(row['ggid']).strip().zfill(8)

                # Nettoyage des valeurs d'entité
                entite_vals = tuple(
                    '' if pd.isna(val) else str(val).strip()
                    for val in (
                        row['entite_niveau_1'], row['entite_niveau_2'],
                        row.get('entite_niveau_3', ''), row.get('entite_niveau_4', ''),
                        row.get('pu_code', ''), row.get('etablissement', '')
                    )
                )


                cursor.execute("""
                    SELECT id_entite FROM dim_entites WHERE
                    entite_niveau_1 = ? AND entite_niveau_2 = ? AND entite_niveau_3 = ? AND entite_niveau_4 = ? AND pu_code = ? AND etablissement = ?
                """, *entite_vals)
                entite_row = cursor.fetchone()

                if entite_row:
                    id_entite = entite_row[0]
                else:
                    cursor.execute("""
                        INSERT INTO dim_entites (entite_niveau_1, entite_niveau_2, entite_niveau_3, entite_niveau_4, pu_code, etablissement)
                        OUTPUT INSERTED.id_entite
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, *entite_vals)
                    id_entite = cursor.fetchone()[0]
                    logging.info(f"✅ Nouvelle entité insérée avec ID {id_entite}")

                # Vérifie si le collaborateur existe
                cursor.execute("SELECT 1 FROM dim_collaborateur WHERE ggid = ?", ggid)
                exists = cursor.fetchone()

                if exists:
                    cursor.execute("SELECT id_entite FROM dim_collaborateur WHERE ggid = ?", ggid)
                    current_id_entite = cursor.fetchone()[0]

                    if current_id_entite != id_entite:
                        # Met à jour l'entité du collaborateur systématiquement
                        cursor.execute("""
                            UPDATE dim_collaborateur
                            SET id_entite = ?, nom = ?, prenom = ?, grade = ?, manager = ?
                            WHERE ggid = ?
                        """, id_entite, row['nom'], row['prenom'], row['grade'], row['manager'], ggid)
                        logging.info(f"🔁 Collaborateur {ggid} mis à jour avec entité {id_entite}")
                else:
                    # Insère le collaborateur
                    cursor.execute("""
                        INSERT INTO dim_collaborateur (ggid, nom, prenom, grade, manager, id_entite)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, ggid, row['nom'], row['prenom'], row['grade'], row['manager'], id_entite)
                    logging.info(f"✅ Collaborateur {ggid} inséré avec entité {id_entite}")


                mois = safe_date(row['mois'])        
                if mois is None:
                    logging.warning(f"⚠️ Mois invalide ou manquant à la ligne {index + 2}, insertion ignorée.")
                    continue

                logging.info(f"Ligne {index + 2} — mois: {mois} (type: {type(mois)})")

                semaine = row.get('semaine')
                mois_file = row.get('mois_file')
                annee = row.get('annee')
                source = row.get('source')
                arve_cis_fr = round(float(row['arve_cis_fr']), 2) if pd.notna(row['arve_cis_fr']) else None

                cursor.execute("""
                               SELECT 1 FROM fact_disponibilite WHERE ggid = ? AND mois = ? AND semaine = ?
                               """, ggid, mois, semaine)
                exists = cursor.fetchone()
                
                if exists:
                    cursor.execute("""
                                UPDATE fact_disponibilite
                                SET gross_capacity = ?, billable = ?, available = ?, training = ?, arve_cis_fr = ?, work_study =?, non_billable =?, 
                                   absence=?, leave =?, management=?, internal_activity=?, business_development=?, recruitment=?,
                                   coordination=?, childcare_leave=?, other_absence=?, sick_leave=?, 
                                   work_accident=?, paid_vacation=?, compensatory_time=?, employer_compensatory_time=?, 
                                   recovery=?, bereavement_leave=?,maternity_leave=?, other_vacation=?, therapeutic_half_time =?,
                                   wedding_leave=?, parental_leave=?, paternity_leave=?, unpaid_vacation=?,
                                   source = ?, semaine = ?, mois_file = ?, annee = ?
                                WHERE ggid = ? AND mois = ? AND semaine = ?
                                """, row['gross_capacity'], row['billable'], row['available'], row['training'], arve_cis_fr, 
                                row['work_study'], row['non_billable'], row['absence'], row['leave'],row['management'], row['internal_activity'], row['business_development'], row['recruitment'],
                                row['coordination'], row['childcare_leave'], row['other_absence'], row['sick_leave'], 
                                row['work_accident'], row['paid_vacation'], row['compensatory_time'], row['employer_compensatory_time'], 
                                row['recovery'], row['bereavement_leave'],row['maternity_leave'], row['other_vacation'], row['therapeutic_half_time'],
                                row['wedding_leave'], row['parental_leave'], row['paternity_leave'], row['unpaid_vacation'],
                                source, semaine, mois_file, annee, ggid, mois)
                    logging.info(f"🔁 Disponibilité mise à jour pour GGID {ggid} au mois {mois}")
                else:
                    cursor.execute("""
                        INSERT INTO fact_disponibilite (ggid, mois, gross_capacity, billable, available, training, arve_cis_fr, work_study, non_billable, absence, leave, 
                                management, internal_activity, business_development, recruitment,
                                   coordination, childcare_leave, other_absence, sick_leave, 
                                   work_accident, paid_vacation, compensatory_time, employer_compensatory_time, 
                                   recovery, bereavement_leave,maternity_leave, other_vacation, therapeutic_half_time ,
                                   wedding_leave, parental_leave, paternity_leave, unpaid_vacation,
                                   source, semaine, mois_file, annee)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?,?,?,?,?,?)""",
                        ggid, mois, row['gross_capacity'], row['billable'], row['available'], row['training'],
                        arve_cis_fr, row['work_study'], row['non_billable'], row['absence'], row['leave'],row['management'], row['internal_activity'], row['business_development'], row['recruitment'],
                                row['coordination'], row['childcare_leave'], row['other_absence'], row['sick_leave'], 
                                row['work_accident'], row['paid_vacation'], row['compensatory_time'], row['employer_compensatory_time'], 
                                row['recovery'], row['bereavement_leave'],row['maternity_leave'], row['other_vacation'], row['therapeutic_half_time'],
                                row['wedding_leave'], row['parental_leave'], row['paternity_leave'], row['unpaid_vacation'],
                                source, semaine, mois_file, annee)
                    logging.info(f"✅ Disponibilité insérée pour GGID {ggid} au mois {mois}")

            conn.commit()
            logging.info("✅ Toutes les données ont été insérées avec succès.")

        except Exception as e:
            conn.rollback()
            logging.error(f"❌ Erreur pendant l'insertion : {e}")
            raise  # Pour que router.py sache que ça a échoué
