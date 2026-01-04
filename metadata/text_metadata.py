import sqlite3
import os
from typing import Dict, Any, Optional

# ----------------------------
# --- HELPERS ---
# ----------------------------

def _read_file_content(path: str) -> str:
    """
    Tente de lire le contenu du fichier avec différents encodages.
    Retourne le contenu en string.
    """
    encodings = ['utf-8', 'latin-1', 'cp1252', 'ascii']
    
    for enc in encodings:
        try:
            with open(path, 'r', encoding=enc) as f:
                return f.read()
        except (UnicodeDecodeError, OSError):
            continue
            
    # Si tout échoue, on tente en binaire avec errors='replace' pour récupérer ce qu'on peut
    try:
        with open(path, 'rb') as f:
            return f.read().decode('utf-8', errors='replace')
    except Exception:
        return ""

def _detect_encoding(path: str) -> str:
    """
    Détection basique de l'encodage pour les métadonnées.
    """
    encodings = ['utf-8', 'latin-1', 'cp1252']
    for enc in encodings:
        try:
            with open(path, 'r', encoding=enc) as f:
                f.read()
                return enc
        except (UnicodeDecodeError, OSError):
            continue
    return "unknown"

# -------------------------
# --- MAIN DATA EXTRACT ---
# -------------------------

def extract_text_metadata_from_path(path: str) -> Dict[str, Any]:
    """
    Extrait les métadonnées statistiques et le contenu des fichiers texte.
    """
    meta = {
        "line_count": 0,
        "word_count": 0,
        "char_count": 0,
        "encoding": "utf-8",
        "mime_detected": "text/plain", # Valeur par défaut
        "Exerpt_hund": None,
        "Exerpt_thou": None,
        "Exerpt_full": None
    }

    if not os.path.exists(path):
        return meta

    try:
        # 1. Détection de l'encodage
        detected_enc = _detect_encoding(path)
        meta["encoding"] = detected_enc

        # 2. Lecture du contenu
        content = _read_file_content(path)
        
        if content:
            # Stats
            meta["char_count"] = len(content)
            meta["line_count"] = len(content.splitlines())
            # Comptage de mots simple
            meta["word_count"] = len(content.split())

            # Extraits
            meta["Exerpt_full"] = content
            meta["Exerpt_thou"] = content[:1000]
            meta["Exerpt_hund"] = content[:100]
            
            # Raffinement simple du Mime Type basé sur l'extension si nécessaire
            # (Optionnel, peut être délégué à Magic_Scan, mais utile d'avoir ici)
            ext = os.path.splitext(path)[1].lower()
            if ext == '.json':
                meta["mime_detected"] = "application/json"
            elif ext == '.py':
                meta["mime_detected"] = "text/x-python"
            elif ext == '.md':
                meta["mime_detected"] = "text/markdown"
            elif ext == '.csv':
                meta["mime_detected"] = "text/csv"
            elif ext in ['.html', '.htm']:
                meta["mime_detected"] = "text/html"
            elif ext == '.xml':
                meta["mime_detected"] = "text/xml"

    except Exception as e:
        print(f"Erreur extraction Text {path}: {e}")

    return meta


# -------------------------
# --- POPULATE DATABASE ---
# -------------------------

def populate_text_metadata(conn: sqlite3.Connection, file_id: int) -> None:
    """
    Lit le chemin du fichier dans la table 'file',
    extrait les métadonnées texte, et insère/met à jour :
      - file_text_metadata
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
    meta = extract_text_metadata_from_path(path)
    
    cur.execute(
        """
        INSERT OR REPLACE INTO file_text_metadata (
            file_id,
            line_count,
            word_count,
            char_count,
            encoding,
            Exerpt_hund,
            Exerpt_thou,
            Exerpt_full
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            file_id,
            meta["line_count"],
            meta["word_count"],
            meta["char_count"],
            meta["encoding"],
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