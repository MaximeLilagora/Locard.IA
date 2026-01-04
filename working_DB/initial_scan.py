import os
import sqlite3
import hashlib
from pathlib import Path

DB_PATH = "working_DB/project_index.db"


def sha256_of_file(filepath: str, chunk_size: int = 8192) -> str:
    """Calcule le hash SHA-256 d'un fichier (lecture par blocs)."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def get_or_create_folder(cur: sqlite3.Cursor, path: str, parent_id: int | None, subcount: int) -> int:
    """
    Insère ou met à jour un dossier dans la table 'folder' et retourne son ID.
    Gère la relation parent_id et le nombre de fichiers directs.
    """
    # On normalise le chemin pour être sûr
    norm_path = os.path.abspath(path)
    
    # SQLite UPSERT pour insérer ou mettre à jour les infos du dossier
    try:
        cur.execute("""
            INSERT INTO folder (path, parent_id, files_subcount)
            VALUES (?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                parent_id = excluded.parent_id,
                files_subcount = excluded.files_subcount
            RETURNING id;
        """, (norm_path, parent_id, subcount))
        
        row = cur.fetchone()
        if row:
            return row[0]
            
    except sqlite3.OperationalError:
        # Fallback pour les vieilles versions de SQLite qui ne supportent pas RETURNING
        cur.execute("""
            INSERT INTO folder (path, parent_id, files_subcount)
            VALUES (?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                parent_id = excluded.parent_id,
                files_subcount = excluded.files_subcount
        """, (norm_path, parent_id, subcount))

    # Récupération de l'ID si pas retourné (ou fallback)
    cur.execute("SELECT id FROM folder WHERE path = ?", (norm_path,))
    res = cur.fetchone()
    if res:
        return res[0]
    else:
        # Ne devrait pas arriver
        raise ValueError(f"Impossible de récupérer l'ID pour le dossier : {norm_path}")


def scan_folder_and_store(folder: str, db_path: str = DB_PATH) -> None:
    """Scan récursivement `folder`, peuple la table 'folder' et stocke/maj les fichiers."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Activation des clés étrangères pour la cohérence
    cur.execute("PRAGMA foreign_keys = ON;")

    # Cache pour éviter de requêter la DB pour chaque parent_id
    # format: { chemin_absolu_dossier: folder_id }
    folder_cache = {}

    abs_scan_root = os.path.abspath(folder)

    for root, dirs, files in os.walk(abs_scan_root):
        current_path = os.path.abspath(root)
        
        # 1. Gestion du Dossier (Table folder)
        # -----------------------------------
        
        # Recherche du parent_id
        parent_path = os.path.dirname(current_path)
        parent_id = None
        
        # Si ce n'est pas la racine absolue du système ou du scan, on cherche le parent
        if current_path != abs_scan_root:
            # Essai via le cache
            parent_id = folder_cache.get(parent_path)
            
            # Si pas dans le cache (ex: parent hors du dossier de scan ou scan partiel), on tente la DB
            if parent_id is None:
                res = cur.execute("SELECT id FROM folder WHERE path = ?", (parent_path,)).fetchone()
                if res:
                    parent_id = res[0]
                    folder_cache[parent_path] = parent_id # Mise en cache

        # Insertion / Maj du dossier actuel
        folder_id = get_or_create_folder(cur, current_path, parent_id, len(files))
        folder_cache[current_path] = folder_id

        # 2. Gestion des Fichiers (Table file)
        # ------------------------------------
        for filename in files:
            full_path = os.path.join(root, filename)

            try:
                st = os.stat(full_path)
            except OSError:
                continue

            size_bytes = st.st_size
            mtime = int(st.st_mtime)

            _, ext = os.path.splitext(filename)
            decl_extension = ext.lower().lstrip(".") if ext else None

            # Calcul du hash (optionnel selon perf, mais présent dans ton code original)
            try:
                hash_sha256 = sha256_of_file(full_path)
            except (OSError, PermissionError):
                hash_sha256 = None

            cur.execute("""
                INSERT INTO file (path, folder_id, size_bytes, mtime, decl_extension, hash_sha256)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(path) DO UPDATE SET
                    folder_id      = excluded.folder_id,
                    size_bytes     = excluded.size_bytes,
                    mtime          = excluded.mtime,
                    decl_extension = excluded.decl_extension,
                    hash_sha256    = excluded.hash_sha256;
            """, (full_path, folder_id, size_bytes, mtime, decl_extension, hash_sha256))

    conn.commit()
    conn.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python initial_scan.py <folder_to_scan>")
        sys.exit(1)
    
    target_folder = sys.argv[1]
    if os.path.exists(target_folder):
        print(f"Lancement du scan sur : {target_folder}")
        scan_folder_and_store(target_folder)
        print("Scan terminé.")
    else:
        print(f"Erreur: Le dossier '{target_folder}' n'existe pas.")