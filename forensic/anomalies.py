import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

def run_forensic_analysis(db_path: str):
    """
    Exécute l'ensemble des 15 indicateurs forensiques sur la base de données SQLite.
    Retourne un dictionnaire contenant les DataFrames et les scalaires pour chaque KPI.
    """
    if not Path(db_path).exists():
        return {"error": "Base de données introuvable."}

    conn = sqlite3.connect(db_path)
    results = {}

    try:
        # ==============================================================================
        # 🚨 GROUPE 1 : DÉTECTION D'ANOMALIES & ANTI-FORENSICS
        # ==============================================================================

        # 1. Extension Spoofing (Masquage d'extension)
        # On cherche les fichiers où l'extension déclarée diffère de l'extension réelle
        query_spoofing = """
            SELECT path, size_bytes, decl_extension, true_extension, mime_detected
            FROM file
            WHERE true_extension IS NOT NULL 
            AND decl_extension != true_extension
            AND decl_extension != ''
        """
        results['spoofing_df'] = pd.read_sql_query(query_spoofing, conn)

        # 2. Timestomping (Incohérence Temporelle - Simplifié)
        # On compare le mtime système (table file) avec la date de création interne (Office/PDF/Image)
        # Note: Cette requête est une approximation, un écart > 24h (86400s) est considéré suspect.
        # SQLite ne gère pas facilement les formats de dates ISO vs Epoch, on fait le calcul grossier.
        query_timestomp = """
            SELECT f.path, f.mtime, meta.created_date as internal_date, 'Office' as source
            FROM file f
            JOIN file_office_metadata meta ON f.id = meta.file_id
            WHERE meta.created_date IS NOT NULL
            UNION
            SELECT f.path, f.mtime, meta.exif_datetime_original as internal_date, 'EXIF' as source
            FROM file f
            JOIN file_image_metadata meta ON f.id = meta.file_id
            WHERE meta.exif_datetime_original IS NOT NULL
        """
        df_time = pd.read_sql_query(query_timestomp, conn)
        # Post-traitement Python pour les dates
        if not df_time.empty:
            df_time['mtime_dt'] = pd.to_datetime(df_time['mtime'], unit='s')
            df_time['internal_dt'] = pd.to_datetime(df_time['internal_date'], errors='coerce')
            df_time['diff_hours'] = (df_time['mtime_dt'] - df_time['internal_dt']).abs().dt.total_seconds() / 3600
            # On garde ceux avec un écart > 24h
            results['timestomping_df'] = df_time[df_time['diff_hours'] > 24].sort_values('diff_hours', ascending=False)
        else:
            results['timestomping_df'] = pd.DataFrame()

        # 3. Zip Bombs / Ratio de compression suspect
        # Ratio > 100 considéré comme très suspect
        query_compression = """
            SELECT f.path, m.compressed_size, m.total_uncompressed_size, m.compression_ratio
            FROM file f
            JOIN file_archive_metadata m ON f.id = m.file_id
            WHERE m.compression_ratio > 50 OR m.total_uncompressed_size > 1073741824 -- 1GB
            ORDER BY m.compression_ratio DESC
        """
        results['zipbomb_df'] = pd.read_sql_query(query_compression, conn)

        # 4. Fichiers Fantômes (Hash collisions / Doublons de contenu)
        # Fichiers avec le même contenu (hash) mais chemins différents
        query_ghosts = """
            SELECT hash_sha256, count(*) as occurences, group_concat(decl_extension, ', ') as extensions
            FROM file
            WHERE hash_sha256 IS NOT NULL
            GROUP BY hash_sha256
            HAVING count(*) > 1
            ORDER BY count(*) DESC
            LIMIT 50
        """
        results['ghost_files_df'] = pd.read_sql_query(query_ghosts, conn)

        # ==============================================================================
        # 🔐 GROUPE 2 : SÉCURITÉ & RISQUE
        # ==============================================================================

        # 5. Exposition aux Secrets (Hardcoded Secrets)
        query_secrets = """
            SELECT f.path, 'Code' as type
            FROM file f JOIN file_code_metadata m ON f.id = m.file_id
            WHERE m.has_secrets = 1
            UNION
            SELECT f.path, 'Text' as type
            FROM file f JOIN file_text_metadata m ON f.id = m.file_id
            WHERE m.has_secrets = 1
        """
        results['secrets_df'] = pd.read_sql_query(query_secrets, conn)

        # 6. Shadow Encryption (Fichiers chiffrés inaccessibles)
        query_crypto = """
            SELECT f.path, 'Archive' as type FROM file f JOIN file_archive_metadata m ON f.id = m.file_id
            WHERE m.is_encrypted = 1 OR m.is_password_protected = 1
            UNION
            SELECT f.path, 'PDF' as type FROM file f JOIN file_pdf_metadata m ON f.id = m.file_id
            WHERE m.is_encrypted = 1
            UNION
            SELECT f.path, 'Database' as type FROM file f JOIN file_database_metadata m ON f.id = m.file_id
            WHERE m.is_encrypted = 1
        """
        results['encrypted_df'] = pd.read_sql_query(query_crypto, conn)

        # 7. Exécutables non signés
        query_unsigned = """
            SELECT f.path, m.architecture, m.original_filename
            FROM file f
            JOIN file_exe_metadata m ON f.id = m.file_id
            WHERE m.is_signed = 0
        """
        results['unsigned_exe_df'] = pd.read_sql_query(query_unsigned, conn)

        # 8. Densité RGPD par dossier (Heatmap)
        # On suppose que rgpd_score_file a été peuplé (sinon 0 par défaut)
        query_gdpr = """
            SELECT fo.path as folder_path, SUM(f.rgpd_score_file) as total_risk_score, COUNT(f.id) as file_count
            FROM file f
            JOIN folder fo ON f.folder_id = fo.id
            WHERE f.rgpd_score_file > 0
            GROUP BY fo.id
            ORDER BY total_risk_score DESC
            LIMIT 20
        """
        results['gdpr_heatmap_df'] = pd.read_sql_query(query_gdpr, conn)

        # ==============================================================================
        # 🕵️ GROUPE 3 : PROFILING & AUTEURS
        # ==============================================================================

        # 9. Heures Silencieuses (Activité suspecte nuit/week-end)
        # SQLite: strftime('%w', ...) 0=Dimanche, 6=Samedi. '%H' 00-23.
        query_silent = """
            SELECT 
                path, 
                datetime(mtime, 'unixepoch', 'localtime') as mod_time,
                size_bytes
            FROM file
            WHERE 
                CAST(strftime('%H', datetime(mtime, 'unixepoch', 'localtime')) AS INT) BETWEEN 22 AND 6
                OR strftime('%w', datetime(mtime, 'unixepoch', 'localtime')) IN ('0', '6')
            ORDER BY mtime DESC
            LIMIT 100
        """
        results['silent_hours_df'] = pd.read_sql_query(query_silent, conn)

        # 10. Auteurs Externes / Multiples
        query_authors = """
            SELECT author, count(*) as doc_count, 'Office' as source
            FROM file_office_metadata
            WHERE author IS NOT NULL
            GROUP BY author
            UNION
            SELECT last_modified_by as author, count(*) as doc_count, 'Office (Modif)' as source
            FROM file_office_metadata
            WHERE last_modified_by IS NOT NULL
            GROUP BY last_modified_by
            UNION
            SELECT author, count(*) as doc_count, 'PDF' as source
            FROM file_pdf_metadata
            WHERE author IS NOT NULL
            GROUP BY author
            ORDER BY doc_count DESC
        """
        results['authors_df'] = pd.read_sql_query(query_authors, conn)

        # 11. Fake Work (Génération rapide)
        # Contenu important mais temps d'édition très faible (ex: < 5 min pour > 1000 mots)
        query_fake = """
            SELECT f.path, m.word_count, m.total_editing_time_sec, m.author
            FROM file f
            JOIN file_office_metadata m ON f.id = m.file_id
            WHERE m.word_count > 1000 
            AND m.total_editing_time_sec > 0 
            AND m.total_editing_time_sec < 300
            ORDER BY m.word_count DESC
        """
        results['fakework_df'] = pd.read_sql_query(query_fake, conn)

        # 12. Empreinte Matérielle (Appareils photo)
        query_camera = """
            SELECT camera_make, camera_model, count(*) as photo_count
            FROM file_image_metadata
            WHERE camera_make IS NOT NULL OR camera_model IS NOT NULL
            GROUP BY camera_make, camera_model
            ORDER BY photo_count DESC
        """
        results['cameras_df'] = pd.read_sql_query(query_camera, conn)

        # ==============================================================================
        # 📊 GROUPE 4 : QUALITÉ & VOLUMÉTRIE
        # ==============================================================================

        # 13. Documents Morts (Zombies > 3 ans)
        three_years_ago = pd.Timestamp.now().timestamp() - (3 * 365 * 24 * 3600)
        query_zombies = f"""
            SELECT path, datetime(mtime, 'unixepoch') as last_mod, size_bytes
            FROM file
            WHERE mtime < {three_years_ago}
            ORDER BY mtime ASC
            LIMIT 50
        """
        results['zombies_df'] = pd.read_sql_query(query_zombies, conn)

        # 14. Dette Technique (Code)
        # Fichiers avec beaucoup de TODO ou peu de commentaires
        query_tech_debt = """
            SELECT f.path, m.todo_count, m.comment_ratio, m.lines_code
            FROM file f
            JOIN file_code_metadata m ON f.id = m.file_id
            WHERE m.todo_count > 5 OR (m.lines_code > 100 AND m.comment_ratio < 0.05)
            ORDER BY m.todo_count DESC
        """
        results['tech_debt_df'] = pd.read_sql_query(query_tech_debt, conn)

        # 15. Dispersion Géographique
        query_geo = """
            SELECT f.path, m.gps_lat, m.gps_lon
            FROM file f
            JOIN file_image_metadata m ON f.id = m.file_id
            WHERE m.gps_lat IS NOT NULL AND m.gps_lon IS NOT NULL
        """
        results['geo_df'] = pd.read_sql_query(query_geo, conn)

    except Exception as e:
        results['error'] = str(e)
    finally:
        conn.close()

    return results

if __name__ == "__main__":
    # Test autonome
    DB_TEST_PATH = "../working_DB/project_index.db"
    if Path(DB_TEST_PATH).exists():
        print(f"Test d'analyse sur {DB_TEST_PATH}...")
        res = run_forensic_analysis(DB_TEST_PATH)
        if 'error' in res:
            print(f"Erreur: {res['error']}")
        else:
            for k, v in res.items():
                if isinstance(v, pd.DataFrame):
                    print(f"✅ {k}: {len(v)} lignes")
    else:
        print("Base de données non trouvée pour le test.")