# metadata/database_metadata.py
import sqlite3
import os
from typing import Dict, Any, List, Optional

# ----------------------------
# --- HELPERS ---
# ----------------------------

def _is_sqlite_header(path: str) -> bool:
    """Vérifie si l'en-tête du fichier correspond à SQLite format 3."""
    try:
        if os.path.getsize(path) < 100:
            return False
        with open(path, 'rb') as f:
            header = f.read(16)
            return header == b'SQLite format 3\x00'
    except Exception:
        return False

def _analyze_sqlite_db(path: str) -> Dict[str, Any]:
    """
    Connecte au fichier SQLite cible et extrait le schéma et les stats.
    """
    stats = {
        "table_count": 0,
        "view_count": 0,
        "index_count": 0,
        "total_rows_est": 0, # Estimation sommaire
        "schema_dump": ""
    }
    
    conn = None
    try:
        # On ouvre en mode lecture seule via URI pour éviter de locker/modifier
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        cur = conn.cursor()
        
        # 1. Récupération des objets (Tables, Views, Index)
        # On exclut les tables internes sqlite_%
        cur.execute("SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")
        items = cur.fetchall()
        
        tables = []
        schema_lines = []
        
        for type_, name, sql in items:
            if type_ == 'table':
                stats["table_count"] += 1
                tables.append(name)
                if sql: schema_lines.append(sql + ";")
            elif type_ == 'view':
                stats["view_count"] += 1
                if sql: schema_lines.append(sql + ";")
            elif type_ == 'index':
                stats["index_count"] += 1
        
        # 2. Comptage rapide des lignes (Max 5 tables pour la perf)
        # On ne fait pas de COUNT(*) sur tout si c'est énorme, juste un échantillon
        for tbl in tables[:5]:
            try:
                cur.execute(f"SELECT COUNT(*) FROM '{tbl}'")
                count = cur.fetchone()[0]
                stats["total_rows_est"] += count
            except:
                pass
                
        # Construction du dump texte (Schema)
        header = f"-- DATABASE SCHEMA DUMP --\n-- Tables: {stats['table_count']}, Views: {stats['view_count']}\n\n"
        stats["schema_dump"] = header + "\n\n".join(schema_lines)

    except sqlite3.Error as e:
        stats["schema_dump"] = f"-- Error reading SQLite DB: {e}"
        print(f"Erreur SQLite {path}: {e}")
    finally:
        if conn:
            conn.close()
            
    return stats

def _detect_other_db_types(path: str) -> str:
    """Détection basique pour d'autres formats DB."""
    try:
        with open(path, 'rb') as f:
            header = f.read(2048)
            
            # MS Access (Standard Jet DB signature)
            if b'Standard Jet DB' in header:
                return "MS Access (Jet)"
            
            # H2 Database
            if b'H2 0.5/B' in header:
                return "H2 Database"
            
            # Berkeley DB (Magic bytes often differ, hard to detect simply)
            
    except:
        pass
    return "Unknown"

# -------------------------
# --- MAIN DATA EXTRACT ---
# -------------------------

def extract_database_metadata_from_path(path: str) -> Dict[str, Any]:
    """
    Extrait les métadonnées de fichiers de base de données.
    """
    meta = {
        "db_type": "Unknown",
        "table_count": 0,
        "row_count": 0, # Total estimé
        "mime_detected": "application/octet-stream",
        "Exerpt_hund": None,
        "Exerpt_thou": None,
        "Exerpt_full": None
    }

    if not os.path.exists(path):
        return meta

    try:
        # 1. Check SQLite
        if _is_sqlite_header(path):
            meta["db_type"] = "SQLite"
            meta["mime_detected"] = "application/vnd.sqlite3"
            
            # Analyse profonde
            sql_stats = _analyze_sqlite_db(path)
            meta["table_count"] = sql_stats["table_count"]
            meta["row_count"] = sql_stats["total_rows_est"]
            
            full_text = sql_stats["schema_dump"]
            meta["Exerpt_full"] = full_text
            meta["Exerpt_thou"] = full_text[:1000]
            meta["Exerpt_hund"] = full_text[:100]
            
        else:
            # Check autres types (juste identification)
            other_type = _detect_other_db_types(path)
            if other_type != "Unknown":
                meta["db_type"] = other_type
                if "Access" in other_type:
                    meta["mime_detected"] = "application/x-msaccess"
                
                info_text = f"DATABASE TYPE: {other_type}\nNo detailed schema extraction available without specific drivers."
                meta["Exerpt_full"] = info_text
                meta["Exerpt_thou"] = info_text
                meta["Exerpt_hund"] = info_text

    except Exception as e:
        print(f"Erreur extraction DB {path}: {e}")

    return meta


# -------------------------
# --- POPULATE DATABASE ---
# -------------------------

def populate_database_metadata(conn: sqlite3.Connection, file_id: int) -> None:
    """
    Lit le chemin du fichier dans la table 'file',
    extrait les métadonnées Database, et insère/met à jour :
      - file_database_metadata
      - file.mime_detected
    """
    cur = conn.cursor()

    row = cur.execute(
        "SELECT path FROM file WHERE id = ?",
        (file_id,),
    ).fetchone()

    if row is None:
        raise ValueError(f"Aucun fichier avec id={file_id}")

    path = row[0]

    # Extraction
    meta = extract_database_metadata_from_path(path)
    
    # Insertion
    cur.execute(
        """
        INSERT OR REPLACE INTO file_database_metadata (
            file_id,
            db_type,
            table_count,
            row_count,
            Exerpt_hund,
            Exerpt_thou,
            Exerpt_full
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            file_id,
            meta["db_type"],
            meta["table_count"],
            meta["row_count"],
            meta["Exerpt_hund"],
            meta["Exerpt_thou"],
            meta["Exerpt_full"]
        ),
    )

    # Mise à jour du MIME détecté
    cur.execute(
        """
        UPDATE file
        SET mime_detected = ?,
            updated_at    = datetime('now')
        WHERE id = ?
        """,
        (meta["mime_detected"], file_id),
    )

    conn.commit()