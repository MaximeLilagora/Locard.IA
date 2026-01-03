# scanner_parallel_ai.py

import os
import hashlib
import json
import sqlite3
import requests
import filetype
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import threading

try:
    from pypdf import PdfReader
except ImportError:
    PdfReader = None

try:
    from docx import Document
except ImportError:
    Document = None

DB_NAME = "commando_index.db"
LLAMA_API_URL = "http://127.0.0.1:8080/v1/chat/completions"
MAX_WORKERS_SCAN = 16
MAX_WORKERS_AI = 12
MAX_TEXT_LENGTH = 1000
BATCH_SIZE = 500
REQUEST_TIMEOUT = 120

ALLOWED_EXTENSIONS = {
    ".txt", ".md",
    ".pdf",
    ".doc", ".docx", ".odt",
}


class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.lock = threading.Lock()
        self.connect()
        self.init_db()

    def connect(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.cursor = self.conn.cursor()

    def close(self):
        if self.conn:
            self.conn.commit()
            self.conn.close()
            self.conn = None
            self.cursor = None

    def init_db(self):
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT,
                path TEXT UNIQUE,
                declared_extension TEXT,
                true_extension TEXT,
                size_bytes INTEGER,
                sha256 TEXT,
                status TEXT,
                text_content TEXT,
                last_scan DATETIME,
                nature TEXT,
                total_cost TEXT,
                ai_summary TEXT,
                ai_author TEXT,
                ai_date_extracted TEXT,
                ai_processed INTEGER DEFAULT 0
            )
        """
        )

        self.cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ai_processed 
            ON files(ai_processed) WHERE ai_processed = 0
        """
        )

        self.cursor.execute("PRAGMA table_info(files)")
        cols = {row[1] for row in self.cursor.fetchall()}

        if "ai_summary" not in cols:
            self.cursor.execute("ALTER TABLE files ADD COLUMN ai_summary TEXT")
        if "ai_author" not in cols:
            self.cursor.execute("ALTER TABLE files ADD COLUMN ai_author TEXT")
        if "ai_date_extracted" not in cols:
            self.cursor.execute(
                "ALTER TABLE files ADD COLUMN ai_date_extracted TEXT"
            )
        if "ai_processed" not in cols:
            self.cursor.execute(
                "ALTER TABLE files ADD COLUMN ai_processed INTEGER DEFAULT 0"
            )
        if "nature" not in cols:
            self.cursor.execute("ALTER TABLE files ADD COLUMN nature TEXT")
        if "total_cost" not in cols:
            self.cursor.execute("ALTER TABLE files ADD COLUMN total_cost TEXT")

        self.conn.commit()

    def batch_upsert(self, records):
        sql = """
        INSERT INTO files (
            file_name,
            path,
            declared_extension,
            true_extension,
            size_bytes,
            sha256,
            status,
            text_content,
            last_scan
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
            file_name = excluded.file_name,
            declared_extension = excluded.declared_extension,
            true_extension = excluded.true_extension,
            size_bytes = excluded.size_bytes,
            sha256 = excluded.sha256,
            status = excluded.status,
            text_content = excluded.text_content,
            last_scan = excluded.last_scan
        """
        with self.lock:
            self.cursor.executemany(sql, records)
            self.conn.commit()

    def get_files_needing_ai(self, limit=None):
        sql = """
            SELECT id, file_name, text_content
            FROM files
            WHERE ai_processed = 0
              AND text_content IS NOT NULL
        """
        params = ()
        if limit:
            sql += " LIMIT ?"
            params = (limit,)

        with self.lock:
            rows = self.cursor.execute(sql, params).fetchall()
        return rows

    def batch_update_ai(self, updates):
        sql = """
            UPDATE files
            SET ai_summary = ?,
                nature = ?,
                total_cost = ?,
                ai_date_extracted = ?,
                ai_processed = 1
            WHERE id = ?
        """
        with self.lock:
            try:
                print(f"[DB] Batch update de {len(updates)} lignes...")
                self.cursor.executemany(sql, updates)
                print(f"[DB] rowcount après update: {self.cursor.rowcount}")
                self.conn.commit()
            except sqlite3.Error as e:
                print(f"❌ Erreur Batch Update: {e}")
                print(f"❌ Données batch (extrait): {updates[:5]}")


def compute_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def extract_text_from_file(path: Path, ext: str) -> str | None:
    ext = ext.lower()
    if ext in {".txt", ".md"}:
        try:
            return path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return None

    if ext == ".pdf":
        if PdfReader is None:
            return None
        try:
            reader = PdfReader(str(path))
            texts = []
            for i, page in enumerate(reader.pages):
                if i >= 20:
                    break
                try:
                    t = page.extract_text()
                except Exception:
                    t = ""
                if t:
                    texts.append(t)
            return "\n".join(texts) if texts else None
        except Exception:
            return None

    if ext in {".docx", ".doc", ".odt"}:
        if Document is None:
            return None
        try:
            doc = Document(str(path))
            texts = [p.text for p in doc.paragraphs if p.text]
            return "\n".join(texts) if texts else None
        except Exception:
            return None

    return None


def process_file_scan(path: Path):
    try:
        if not path.is_file():
            return None

        declared_ext = path.suffix.lower()
        if declared_ext not in ALLOWED_EXTENSIONS:
            return None

        kind = None
        try:
            kind = filetype.guess(str(path))
        except Exception:
            kind = None

        if kind and kind.extension:
            true_ext = "." + kind.extension.lower()
        else:
            true_ext = declared_ext

        size_bytes = path.stat().st_size
        sha256 = compute_sha256(path)

        text_content = extract_text_from_file(path, declared_ext)
        if text_content:
            text_content = text_content[:MAX_TEXT_LENGTH]
            status = "SCANNED"
        else:
            status = "NO_TEXT"

        last_scan = datetime.now().isoformat(timespec="seconds")

        return (
            path.name,
            str(path),
            declared_ext,
            true_ext,
            size_bytes,
            sha256,
            status,
            text_content,
            last_scan,
        )
    except Exception as e:
        print(f"❌ Erreur scan fichier {path}: {e}")
        return None


def ask_llama_worker(data_pack):
    file_id, text_snippet = data_pack

    promptai = f"""Détermine la nature administrative et légale du document en 1 phrase de 30 mots maximum 
(Contrat de prestation, Facture, Fiche de paie, Devis, Dossier, etc.) pour ce document :

{text_snippet[:MAX_TEXT_LENGTH]}

Tu dois répondre UNIQUEMENT avec un JSON valide de la forme :

{{
  "Nature": "votre résumé en une phrase",
  "Montant_TTC": "montant total TTC en euros (nombre ou chaîne), ou null si non applicable"
}}

- Si c'est un contrat ou un devis, remplis "Montant_TTC" avec le montant total TTC trouvé dans le texte.
- Sinon, mets "Montant_TTC" à null.
- Répond uniquement en français.
"""

    payload = {
        "model": "local-model",
        "messages": [
            {"role": "user", "content": promptai}
        ],
        "stream": False,
        "max_tokens": 150,
        "temperature": 0.0,
    }

    try:
        print(f"[LLAMA] Envoi requête pour file_id={file_id}")
        response = requests.post(
            LLAMA_API_URL, json=payload, timeout=REQUEST_TIMEOUT
        )
        print(f"[LLAMA] HTTP {response.status_code} pour file_id={file_id}")
        response.raise_for_status()

        result_json = response.json()
        print("[LLAMA] Réponse brute:", result_json)

        content_str = result_json["choices"][0]["message"]["content"]
        print("[LLAMA] content_str:", content_str)

        try:
            content = json.loads(content_str)
        except Exception as e:
            print(f"[LLAMA][ERREUR JSON] file_id={file_id}: {e}")
            return file_id, {
                "Nature": f"Erreur JSON: {e}",
                "Montant_TTC": None,
                "raw": content_str,
            }

        return file_id, content

    except Exception as e:
        print(f"[LLAMA][ERREUR HTTP] file_id={file_id}: {e}")
        try:
            print("[LLAMA][ERREUR HTTP] body:", response.text[:500])
        except Exception:
            pass
        return file_id, {"Nature": f"Error: {str(e)}", "Montant_TTC": None}


def main():
    print("\n🚀 COMMANDO AI — BEAST MODE")
    print(f"⚡ Config: {MAX_WORKERS_AI} workers IA | {MAX_WORKERS_SCAN} workers scan")
    print("🎯 Target: 10 doc/s\n")

    db = DatabaseManager(DB_NAME)

    target = input(
        "📁 Dossier à scanner (Entrée pour passer à l'IA) : "
    ).strip().strip('"')
    if target and os.path.exists(target):
        files = [p for p in Path(target).rglob("*") if p.is_file()]
        print(f"📡 Scan disque ({len(files)} fichiers)...")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS_SCAN) as executor:
            results = []
            for result in tqdm(
                executor.map(process_file_scan, files),
                total=len(files),
                unit="file",
            ):
                if result:
                    results.append(result)

                if len(results) >= BATCH_SIZE:
                    db.batch_upsert(results)
                    results = []

            if results:
                db.batch_upsert(results)

    rows = db.get_files_needing_ai()
    if not rows:
        print("✅ Tout est à jour.")
        db.close()
        return

    print(f"\n🧠 Lancement de l'IA sur {len(rows)} documents...")
    print(f"⏱️  Estimation: {len(rows) / 10:.1f}s à 10 doc/s\n")

    tasks = [(row["id"], row["text_content"]) for row in rows]
    updates_batch = []

    start_time = datetime.now()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_AI) as executor:
        future_to_file = {
            executor.submit(ask_llama_worker, task): task for task in tasks
        }

        for future in tqdm(
            as_completed(future_to_file), total=len(tasks), unit="doc"
        ):
            file_id, result = future.result()

            nature = str(result.get("Nature") or "")
            montant = str(result.get("Montant_TTC") or "")
            summary = nature[:500]
            date_extracted = datetime.now().isoformat(timespec="seconds")

            updates_batch.append(
                (summary, nature, montant, date_extracted, file_id)
            )

            if len(updates_batch) >= BATCH_SIZE:
                db.batch_update_ai(updates_batch)
                updates_batch = []

    if updates_batch:
        db.batch_update_ai(updates_batch)

    elapsed = (datetime.now() - start_time).total_seconds()
    speed = len(tasks) / elapsed if elapsed > 0 else 0.0

    print(f"\n✅ Analyse terminée en {elapsed:.1f}s")
    print(f"🚀 Vitesse réelle: {speed:.2f} doc/s")

    print("\n🧪 Vérification rapide en base :")
    with db.lock:
        rows_debug = db.cursor.execute(
            "SELECT id, file_name, nature, total_cost, ai_summary, ai_processed "
            "FROM files ORDER BY id DESC LIMIT 10"
        ).fetchall()
        for r in rows_debug:
            print(dict(r))

    db.close()


if __name__ == "__main__":
    main()
