import os
import sqlite3
import hashlib

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


def scan_folder_and_store(folder: str, db_path: str = DB_PATH) -> None:
    """Scan récursivement `folder` et stocke/maj les infos dans la base SQLite."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    for root, dirs, files in os.walk(folder):
        for filename in files:
            full_path = os.path.join(root, filename)

            try:
                st = os.stat(full_path)
            except OSError:
                # Fichier inaccessible (droits, supprimé, etc.)
                continue

            size_bytes = st.st_size
            mtime = int(st.st_mtime)  # timestamp epoch (secondes)

            _, ext = os.path.splitext(filename)
            decl_extension = ext.lower().lstrip(".") if ext else None

            try:
                hash_sha256 = sha256_of_file(full_path)
            except (OSError, PermissionError):
                hash_sha256 = None

            cur.execute("""
                INSERT INTO file (path, size_bytes, mtime, decl_extension, hash_sha256)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(path) DO UPDATE SET
                    size_bytes     = excluded.size_bytes,
                    mtime          = excluded.mtime,
                    decl_extension = excluded.decl_extension,
                    hash_sha256    = excluded.hash_sha256;
            """, (full_path, size_bytes, mtime, decl_extension, hash_sha256))

    conn.commit()
    conn.close()


if __name__ == "__main__":
    # pour éventuels tests en ligne de commande
    import sys
    if len(sys.argv) != 2:
        print("Usage: python initial_scan.py <folder_to_scan>")
        sys.exit(1)
    scan_folder_and_store(sys.argv[1])