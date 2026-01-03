# scanner_parallel_ai.py — CommandoAI BEAST MODE
# M3 Ultra 256GB - Target: 10 doc/s

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
except ImportError: PdfReader = None
try:
    from docx import Document
except ImportError: Document = None

# --------------------------------
# --- CONFIGURATION BEAST MODE ---
# --------------------------------

DB_NAME = "commando_index.db"
LLAMA_API_URL = "http://127.0.0.1:8080/v1/chat/completions"

# ⚡ CRITICAL PARAMETERS // ADJUST DEPENDING OF YOUR HARDWARE SPECS
MAX_WORKERS_SCAN = 16        # tu peux monter sans risque, SSD + gros CPU
MAX_WORKERS_AI = 12          # pareil que --parallel au début
MAX_TEXT_LENGTH = 1000       # OK pour l’instant
BATCH_SIZE = 500             # tu peux augmenter un peu pour moins de commits
REQUEST_TIMEOUT = 120        # laisse respirer quand ça démarre

# DO NOT FORGET TO LAUNCH OLLAMA SERVER WITH OLLAMA_NUM_PARALLEL=(MAX_WORKERS_AI) OLLAMA_MAX_LOADED_MODELS=1 ollama serve
# NO MORE THAN 8 AI WORKERS

ALLOWED_EXTENSIONS = {
    '.txt', '.md',
    '.pdf',
    '.doc', '.docx', '.odt'
}

# SERVER START COMMAND
# Pr+++ cd ~/llama.cpp/build ./bin/llama-server -m /Users/maxime/Models/qwen2.5-32b-instruct-q5_k_m-00001-of-00006.gguf -ngl 999 -c 4096 --threads 24 --parallel 12 --host 127.0.0.1 --port 8080
# Pr+ cd ~/llama.cpp/build ./bin/llama-server -m /Users/maxime/Models/Meta-Llama-3.1-8B-Instruct-Q5_K_M.gguf -ngl 999 -c 4096 --threads 24 --parallel 12 --host 127.0.0.1 --port 8080
# 

# ----------------------------------------
# --- GESTION DB THREAD-SAFE with LOCK ---
# ----------------------------------------

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.lock = threading.Lock()  # 🔒 THREAD-SAFE PROTECTION
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

    def init_db(self):
        self.cursor.execute('''
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
                ai_date_extracted TEXT,
                ai_processed INTEGER DEFAULT 0
            )
        ''')
        
        # Indexing
        self.cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_ai_processed 
            ON files(ai_processed) WHERE ai_processed = 0
        ''')
        
        try:
            self.cursor.execute("SELECT ai_summary FROM files LIMIT 1")
        except sqlite3.OperationalError:
            self.cursor.execute("ALTER TABLE files ADD COLUMN ai_summary TEXT")
            self.cursor.execute("ALTER TABLE files ADD COLUMN ai_author TEXT")
            self.cursor.execute("ALTER TABLE files ADD COLUMN ai_date_extracted TEXT")
            self.cursor.execute("ALTER TABLE files ADD COLUMN ai_processed INTEGER DEFAULT 0")
        self.conn.commit()

    def upsert_file(self, data):
        sql = '''
            INSERT INTO files (file_name, path, declared_extension, true_extension, size_bytes, sha256, status, text_content, last_scan)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                size_bytes=excluded.size_bytes,
                sha256=excluded.sha256,
                text_content=excluded.text_content,
                last_scan=excluded.last_scan
        '''
        with self.lock:  # 🔒 Thread-safe
            try:
                self.cursor.execute(sql, (
                    data['name'], data['path'], data['declared_ext'], data['true_ext'],
                    data['size_bytes'], data['hash'], data['status'], data['text'],
                    datetime.now().isoformat()
                ))
                self.conn.commit()
            except sqlite3.Error as e:
                print(f"⚠️ Erreur DB: {e}")

    def batch_upsert(self, data_list):
        """Insertion par batch - BEAUCOUP plus rapide"""
        sql = '''
            INSERT INTO files (file_name, path, declared_extension, true_extension, size_bytes, sha256, status, text_content, last_scan)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                size_bytes=excluded.size_bytes,
                sha256=excluded.sha256,
                text_content=excluded.text_content,
                last_scan=excluded.last_scan
        '''
        with self.lock:
            try:
                values = [(
                    d['name'], d['path'], d['declared_ext'], d['true_ext'],
                    d['size_bytes'], d['hash'], d['status'], d['text'],
                    datetime.now().isoformat()
                ) for d in data_list]
                self.cursor.executemany(sql, values)
                self.conn.commit()
            except sqlite3.Error as e:
                print(f"⚠️ Erreur Batch DB: {e}")

    def update_ai_analysis(self, file_id, analysis):
        summary = str(analysis.get('Nature', 'Pas de résumé'))[:500]  # SA lenght limit
        
        sql = '''
            UPDATE files SET 
                ai_summary = ?, ai_processed = 1
            WHERE id = ?
        '''
        with self.lock:  # 🔒 Thread-safe
            try:
                self.cursor.execute(sql, (summary, file_id))
                self.conn.commit()
            except sqlite3.Error as e:
                print(f"❌ Erreur Update IA ID {file_id}: {e}")

    def batch_update_ai(self, updates):
        """Mise à jour par batch"""
        sql = '''UPDATE files SET ai_summary = ?, ai_processed = 1 WHERE id = ?'''
        with self.lock:
            try:
                self.cursor.executemany(sql, updates)
                self.conn.commit()
            except sqlite3.Error as e:
                print(f"❌ Erreur Batch Update: {e}")

    def get_files_needing_ai(self):
        with self.lock:
            return self.cursor.execute('''
                SELECT id, file_name, text_content 
                FROM files 
                WHERE ai_processed = 0 
            ''').fetchall()

# -----------------------
# --- FILE PROCESSING ---
# -----------------------

def process_file_scan(path_obj):
    full_path = str(path_obj)
    ext = path_obj.suffix.lower()

    if ext not in ALLOWED_EXTENSIONS:
        return None

    res = {
        "name": path_obj.name, "path": full_path, 
        "declared_ext": ext, "true_ext": ext,
        "size_bytes": path_obj.stat().st_size, "hash": None, 
        "text": "", "status": "scanned"
    }

    # Hash - Inactive for now. 
    # try:
    #     sha256 = hashlib.sha256()
    #     with open(path_obj, 'rb') as f:
    #         while chunk := f.read(8192): sha256.update(chunk)
    #     res["hash"] = sha256.hexdigest()
    # except: pass

    try:
        txt = ""
        if ext in ['.txt', '.md']:
            with open(path_obj, "r", encoding="utf-8", errors="ignore") as f: 
                txt = f.read(MAX_TEXT_LENGTH)  # KEY Value for context loading
        elif ext == ".pdf" and PdfReader:
            reader = PdfReader(full_path)
            for page in reader.pages[:2]:  # For now, we only check the 2st pages. This should be a dynamic variable, depending of the lenght of the document. Maybe a further algorithm ?
                txt += (page.extract_text() or "")
                if len(txt) >= MAX_TEXT_LENGTH: break
        elif ext in [".docx", ".doc"] and Document:
            doc = Document(full_path)
            txt = "\n".join([p.text for p in doc.paragraphs[:10]])  # 10 first paragraphs for now. Same questionning than reader.pages
        res["text"] = txt[:MAX_TEXT_LENGTH].strip()
    except: 
        res["status"] = "read_error"

    return res

def ask_llama_worker(data_pack):
    file_id, text_snippet = data_pack

    promptai = f"""Détermine la nature administrative et légale du document en 1 phrase de 30 mots maximum (Contrat de prestation, Facture, Fiche de paie, Devis, Dossier)... de ce document : 

{text_snippet[:1000]}

Tu dois répondre UNIQUEMENT avec un JSON valide de la forme :
{{
  "Nature": "votre résumé en une phrase",
  "Montant_TTC": "montant total TTC en euros (nombre ou chaîne), ou null si non applicable"
}}.
Répond uniquement en français.
"""

    payload = {
        # facultatif mais je le mets, au cas où ta version de llama-server le demande
        "model": "Meta-Llama-3.1-8B-Instruct-Q5_K_M",
        "messages": [
            {"role": "user", "content": promptai}
        ],
        "stream": False,
        "max_tokens": 150,
        "temperature": 0.0,
        # ⚠️ à COMMENTER si ça pose problème :
        # "response_format": { "type": "json_object" },
    }

    try:
        print(f"[LLAMA] Envoi requête pour file_id={file_id}")
        response = requests.post(LLAMA_API_URL, json=payload, timeout=REQUEST_TIMEOUT)
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
            # on peut au moins renvoyer le texte brut pour debug
            return file_id, {"Nature": f"Erreur JSON: {e}", "raw": content_str}

        return file_id, content

    except Exception as e:
        print(f"[LLAMA][ERREUR HTTP] file_id={file_id}: {e}")
        try:
            print("[LLAMA][ERREUR HTTP] body:", response.text[:500])
        except Exception:
            pass
        return file_id, {"Nature": f"Error: {str(e)}"}


# --- MAIN OPTIMISÉ ---
def main():
    print(f"\n🚀 COMMANDO AI — BEAST MODE")
    print(f"⚡ Config: {MAX_WORKERS_AI} workers IA | {MAX_WORKERS_SCAN} workers scan")
    print(f"🎯 Target: 10 doc/s\n")
    
    db = DatabaseManager(DB_NAME)

    # 1. SCAN DISQUE OPTIMISÉ
    target = input("📁 Dossier à scanner (Entrée pour passer à l'IA) : ").strip().strip('"')
    if target and os.path.exists(target):
        files = [p for p in Path(target).rglob("*") if p.is_file()]
        print(f"📡 Scan disque ({len(files)} fichiers)...")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS_SCAN) as executor:
            results = []
            for result in tqdm(executor.map(process_file_scan, files), total=len(files), unit="file"):
                if result:
                    results.append(result)
                
                # Écriture par batch
                if len(results) >= BATCH_SIZE:
                    db.batch_upsert(results)
                    results = []
            
            # Derniers fichiers
            if results:
                db.batch_upsert(results)

    # 2. Parallel Analysis
    rows = db.get_files_needing_ai()
    if not rows:
        print("✅ Tout est à jour.")
        db.close()
        return

    print(f"\n🧠 Lancement de l'IA sur {len(rows)} documents...")
    print(f"⏱️  Estimation: {len(rows) / 10:.1f}s à 10 doc/s\n")

    tasks = [(row['id'], row['text_content']) for row in rows]
    updates_batch = []

    start_time = datetime.now()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_AI) as executor:
        future_to_file = {executor.submit(ask_llama_worker, task): task for task in tasks}

        for future in tqdm(as_completed(future_to_file), total=len(tasks), unit="doc"):
            file_id, result = future.result()
            
            # Incrementing batch indicator
            summary = str(result.get('Nature', 'Erreur'))[:500]
            updates_batch.append((summary, file_id))
            
            # Per batch writting
            if len(updates_batch) >= BATCH_SIZE:
                db.batch_update_ai(updates_batch)
                updates_batch = []
    
    # Dernières updates
    if updates_batch:
        db.batch_update_ai(updates_batch)

    elapsed = (datetime.now() - start_time).total_seconds()
    speed = len(tasks) / elapsed if elapsed > 0 else 0

    print(f"\n✅ Analyse terminée en {elapsed:.1f}s")
    print(f"🚀 Vitesse réelle: {speed:.2f} doc/s")
    
    db.close()

if __name__ == "__main__":
    main()
