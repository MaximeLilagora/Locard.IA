import sqlite3
import os
from pathlib import Path
from datetime import datetime

# Importation des collecteurs spécifiques
try:
    from metadata.image_metadata import populate_image_metadata
    from metadata.pdf_metadata import populate_pdf_metadata
    from metadata.office_metadata import populate_office_metadata
    from metadata.video_metadata import populate_video_metadata
    from metadata.audio_metadata import populate_audio_metadata
    from metadata.archive_metadata import populate_archive_metadata
    from metadata.exe_metadata import populate_exe_metadata
    from metadata.sourcecode_metadata import populate_code_metadata
    from metadata.text_metadata import populate_text_metadata
    from metadata.tabulardata_metadata import populate_tabulardata_metadata
    from metadata.database_metadata import populate_database_metadata
    from metadata.img_dsk_metadata import populate_disk_image_metadata
    from metadata.ddd_metadata import populate_3d_metadata
except ImportError as e:
    print(f"Erreur d'import des modules metadata : {e}")

# ---------------------------------------------------------
# --- MAPPING EXTENSIONS -> FONCTIONS DE POPULATION ---
# ---------------------------------------------------------

META_ROUTER = {
    # --- IMAGES ---
    '.jpg': populate_image_metadata, '.jpeg': populate_image_metadata,
    '.png': populate_image_metadata, '.gif': populate_image_metadata,
    '.bmp': populate_image_metadata, '.tiff': populate_image_metadata,
    '.tif': populate_image_metadata, '.webp': populate_image_metadata,
    '.ico': populate_image_metadata, '.svg': populate_image_metadata,
    '.xcf': populate_image_metadata,  # GIMP
    '.heic': populate_image_metadata,
    
    # --- PDF ---
    '.pdf': populate_pdf_metadata,
    
    # --- OFFICE & DOCS ---
    '.docx': populate_office_metadata, '.doc': populate_office_metadata,
    '.xlsx': populate_office_metadata, '.xls': populate_office_metadata,
    '.pptx': populate_office_metadata, '.ppt': populate_office_metadata,
    '.odt': populate_office_metadata, '.ods': populate_office_metadata,
    '.odp': populate_office_metadata, '.rtf': populate_office_metadata,

    # --- AUDIO ---
    '.mp3': populate_audio_metadata, '.wav': populate_audio_metadata,
    '.flac': populate_audio_metadata, '.ogg': populate_audio_metadata,
    '.m4a': populate_audio_metadata, '.aac': populate_audio_metadata,
    '.wma': populate_audio_metadata,

    # --- VIDEO ---
    '.mp4': populate_video_metadata, '.mkv': populate_video_metadata,
    '.avi': populate_video_metadata, '.mov': populate_video_metadata,
    '.webm': populate_video_metadata, '.flv': populate_video_metadata,
    '.m4v': populate_video_metadata, '.wmv': populate_video_metadata,

    # --- ARCHIVES ---
    '.zip': populate_archive_metadata, '.rar': populate_archive_metadata,
    '.tar': populate_archive_metadata, '.gz': populate_archive_metadata,
    '.7z': populate_archive_metadata, '.bz2': populate_archive_metadata,
    '.xz': populate_archive_metadata, '.iso': populate_archive_metadata, # ISO est souvent traité comme archive ou img

    # --- EXECUTABLES & BINAIRES ---
    '.exe': populate_exe_metadata, '.dll': populate_exe_metadata,
    '.sys': populate_exe_metadata, '.msi': populate_exe_metadata,
    '.bin': populate_exe_metadata, '.elf': populate_exe_metadata,
    '.so': populate_exe_metadata, '.dylib': populate_exe_metadata,
    '.abi3.so': populate_exe_metadata, '.pak': populate_exe_metadata,
    '.dat': populate_exe_metadata, '.sav': populate_exe_metadata, # IDL Save / Jeux

    # --- CODE SOURCE & WEB ---
    '.py': populate_code_metadata, '.pyi': populate_code_metadata, '.pyx': populate_code_metadata,
    '.js': populate_code_metadata, '.mjs': populate_code_metadata, '.ts': populate_code_metadata,
    '.html': populate_code_metadata, '.htm': populate_code_metadata,
    '.css': populate_code_metadata, '.scss': populate_code_metadata,
    '.java': populate_code_metadata,
    '.c': populate_code_metadata, '.h': populate_code_metadata,
    '.cpp': populate_code_metadata, '.hpp': populate_code_metadata,
    '.php': populate_code_metadata,
    '.rb': populate_code_metadata,
    '.go': populate_code_metadata,
    '.rs': populate_code_metadata,
    '.sh': populate_code_metadata, '.bash': populate_code_metadata, '.bat': populate_code_metadata,
    '.pl': populate_code_metadata, '.pm': populate_code_metadata,
    '.lua': populate_code_metadata, '.sql': populate_code_metadata,
    
    # --- TEXTE / CONFIG / LEGAL ---
    '.txt': populate_text_metadata, '.md': populate_text_metadata,
    '.markdown': populate_text_metadata, '.rst': populate_text_metadata,
    '.json': populate_text_metadata, '.xml': populate_text_metadata,
    '.yaml': populate_text_metadata, '.yml': populate_text_metadata,
    '.toml': populate_text_metadata, '.ini': populate_text_metadata,
    '.cfg': populate_text_metadata, '.conf': populate_text_metadata,
    '.log': populate_text_metadata,
    '.sample': populate_text_metadata,
    '.man': populate_text_metadata, '.7': populate_text_metadata,
    '.apache': populate_text_metadata, '.bsd': populate_text_metadata,
    '.typed': populate_text_metadata,
    
    # --- DATA TABULAIRE ---
    '.csv': populate_tabulardata_metadata, '.tsv': populate_tabulardata_metadata,
    '.parquet': populate_tabulardata_metadata,
    '.nc': populate_tabulardata_metadata, # NetCDF (tentative)

    # --- BASES DE DONNEES ---
    '.sqlite': populate_database_metadata, '.db': populate_database_metadata,
    '.db3': populate_database_metadata, '.sqlite3': populate_database_metadata,
    '.mdb': populate_database_metadata, '.accdb': populate_database_metadata,

    # --- 3D ---
    '.obj': populate_3d_metadata, '.stl': populate_3d_metadata,
    '.fbx': populate_3d_metadata, '.gltf': populate_3d_metadata,
    '.glb': populate_3d_metadata, '.ply': populate_3d_metadata,
    '.step': populate_3d_metadata, '.stp': populate_3d_metadata,

    # --- IMAGES DISQUE ---
    '.img': populate_disk_image_metadata,
    '.vhd': populate_disk_image_metadata,
    '.vmdk': populate_disk_image_metadata,
    '.dmg': populate_disk_image_metadata,

    # --- NOMS EXACTS (Mappés comme des "extensions" pour le dispatch) ---
    'makefile': populate_code_metadata,
    'dockerfile': populate_code_metadata,
    'jenkinsfile': populate_code_metadata,
    'gemfile': populate_code_metadata,
    'vagrantfile': populate_code_metadata,
    'requirements.txt': populate_code_metadata,
    'pipfile': populate_code_metadata,
    'license': populate_text_metadata,
    'license.txt': populate_text_metadata,
    'readme': populate_text_metadata,
    'notice': populate_text_metadata,
}

# Liste noire (fichiers système / métadonnées internes à ignorer)
IGNORED_FILES = {
    '.ds_store', '.localized', 'thumbs.db', 'desktop.ini', 
    '.metadata', '.recommenders', 'pkginfo', '.gitignore', '.gitattributes',
    'record', 'wheel', 'metadata', 'installer', 'requested', 'description', 'exclude'
}

def dispatch_metadata_extraction(conn: sqlite3.Connection, file_id: int, extension: str) -> str:
    """
    Oriente vers le bon collecteur en fonction de l'extension (ou du nom de fichier passé comme extension).
    Retourne un statut (str).
    """
    if not extension:
        return "SKIPPED (No Extension)"

    ext_norm = extension.lower()
    populate_func = META_ROUTER.get(ext_norm)

    if populate_func:
        try:
            populate_func(conn, file_id)
            return f"SUCCESS ({populate_func.__module__})"
        except Exception as e:
            return f"ERROR: {e}"
    else:
        return "SKIPPED (No Collector)"


def run_global_metadata_population(db_path: str, progress_callback=None):
    """
    Fonction principale appelée par l'UI.
    Parcourt la table `file` et lance l'extraction pour chaque entrée.
    Génère un fichier de log 'metadata_scan_log.txt' dans le même dossier que la DB.
    """
    if not os.path.exists(db_path):
        if progress_callback: progress_callback(0, 0, None, None, "DB Not Found")
        return

    # Définition du chemin du log (même dossier que la DB)
    log_path = Path(db_path).parent / "metadata_scan_log.txt"

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 1. Sélectionner les fichiers
    query = """
        SELECT id, path, true_extension, decl_extension 
        FROM file
        ORDER BY id
    """
    cur.execute(query)
    rows = cur.fetchall()
    
    total = len(rows)
    
    # Ouverture du fichier de log
    try:
        with open(log_path, "w", encoding="utf-8") as log_file:
            log_file.write(f"--- START SCAN: {datetime.now()} ---\n")
            log_file.write(f"DB Path: {db_path}\n")
            log_file.write("-" * 50 + "\n")

            if total == 0:
                msg = "Empty DB - No files to scan."
                log_file.write(msg + "\n")
                if progress_callback: progress_callback(0, 0, None, msg)
                conn.close()
                return

            # --- Initialisation des compteurs ---
            count_ok = 0
            count_ignored = 0
            count_error = 0

            for i, row in enumerate(rows, start=1):
                file_id = row[0]
                f_path = row[1]
                true_ext = row[2]
                decl_ext = row[3]
                
                filename = os.path.basename(f_path)
                low_filename = filename.lower()
                status = "UNKNOWN"

                # A. Vérification fichiers ignorés (Système)
                if low_filename in IGNORED_FILES or (true_ext and true_ext.lower() in IGNORED_FILES):
                    status = "SKIPPED (Ignored System File)"
                
                # B. Vérification par nom exact (ex: Makefile, LICENSE)
                elif low_filename in META_ROUTER:
                    # On passe le nom exact comme "extension" au dispatcher
                    status = dispatch_metadata_extraction(conn, file_id, low_filename)

                # C. Vérification standard par extension
                else:
                    # Logique de choix d'extension
                    target_ext = None
                    if true_ext and true_ext.strip():
                        target_ext = true_ext.strip()
                    elif decl_ext and decl_ext.strip():
                        target_ext = decl_ext.strip()
                        if not target_ext.startswith('.'):
                            target_ext = '.' + target_ext
                    
                    status = dispatch_metadata_extraction(conn, file_id, target_ext)

                # --- Mise à jour des compteurs ---
                if status.startswith("SUCCESS"):
                    count_ok += 1
                elif status.startswith("SKIPPED"):
                    count_ignored += 1
                else:
                    # On considère tout le reste (ERROR, ou autre) comme erreur
                    count_error += 1

                # Écriture dans le log
                log_line = f"[{status}] : {filename}"

                # Écriture dans le log
                log_line = f"[{status}] : {filename}"
                log_file.write(log_line + "\n")
                
                # Pour s'assurer que le log s'écrit en temps réel
                log_file.flush()

                # Callback UI
                if progress_callback:
                    progress_callback(i, total, filename, status)
            
            log_file.write("-" * 50 + "\n")
            log_file.write(f"--- END SCAN: {datetime.now()} ---\n")
            
            # --- Résumé des compteurs ---
            log_file.write(f"RESULTATS : OK={count_ok} | IGNORES={count_ignored} | ERREURS={count_error}\n")
            log_file.write(f"--- END SCAN: {datetime.now()} ---\n")

    except Exception as e:
        print(f"Impossible d'écrire le fichier de log : {e}")
    finally:
        conn.close()