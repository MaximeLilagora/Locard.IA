# metadata/tabulardata_metadata.py
import sqlite3
import os
import csv
import json
from typing import Dict, Any, List, Optional

# On tente d'utiliser Pandas pour la robustesse (Excel, Parquet, CSV complexes)
try:
    import pandas as pd
    HAVE_PANDAS = True
except ImportError:
    HAVE_PANDAS = False

# ----------------------------
# --- HELPERS ---
# ----------------------------

def _analyze_csv_fallback(path: str) -> Dict[str, Any]:
    """Analyse CSV légère avec la lib standard (si Pandas absent)."""
    stats = {
        "row_count": 0,
        "column_count": 0,
        "columns_list": [],
        "delimiter": ",",
        "preview_str": ""
    }
    
    try:
        # Sniffer le dialecte
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            sample = f.read(2048)
            f.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample)
                stats["delimiter"] = dialect.delimiter
                has_header = csv.Sniffer().has_header(sample)
            except csv.Error:
                # Fallback valeurs par défaut
                dialect = csv.excel
                has_header = True

            reader = csv.reader(f, dialect)
            
            rows = []
            header_found = False
            
            for i, row in enumerate(reader):
                # On capture les 5 premières lignes pour la preview
                if i < 6:
                    rows.append(row)
                
                # Gestion Header
                if i == 0:
                    stats["column_count"] = len(row)
                    if has_header:
                        stats["columns_list"] = row
                        header_found = True
                    else:
                        # Génère des noms génériques Col_1, Col_2...
                        stats["columns_list"] = [f"Col_{k+1}" for k in range(len(row))]
                
                stats["row_count"] += 1
            
            # Construction de la preview simple
            preview_lines = []
            if header_found and rows:
                preview_lines.append(" | ".join(map(str, stats["columns_list"])))
                preview_lines.append("-" * 20)
                # On saute la 1ère ligne de rows si c'était le header
                start_idx = 1
            else:
                start_idx = 0
            
            for r in rows[start_idx:]:
                preview_lines.append(" | ".join(map(str, r)))
            
            stats["preview_str"] = "\n".join(preview_lines)

    except Exception as e:
        print(f"Erreur fallback CSV {path}: {e}")
        
    return stats

def _analyze_with_pandas(path: str, ext: str) -> Dict[str, Any]:
    """Analyse robuste via Pandas (CSV, Excel, Parquet, JSON, etc.)."""
    stats = {
        "row_count": 0,
        "column_count": 0,
        "columns_list": [],
        "delimiter": None,
        "sheet_count": 0,
        "sheet_names": [],
        "preview_str": ""
    }
    
    df = None
    
    try:
        if ext == '.csv':
            # Pandas détecte souvent bien le séparateur, sinon on peut utiliser 'python' engine
            try:
                df = pd.read_csv(path, sep=None, engine='python', nrows=1000) # On lit un sample pour les métadonnées rapides
                # Pour le row_count total exact, il faut tout lire ou parser le fichier, 
                # ici on lit tout si fichier < 100MB, sinon on estime ou on garde le sample?
                # Pour être précis on va essayer de lire juste les métadonnées si possible.
                # Relecture complète pour le count exact si pas trop gros
                if os.path.getsize(path) < 50 * 1024 * 1024: # 50MB limit
                    full_df = pd.read_csv(path, sep=None, engine='python', usecols=[0])
                    stats["row_count"] = len(full_df)
                else:
                    # Estimation ou bornée au sample
                    stats["row_count"] = 1000 # Placeholder si trop gros
            except:
                pass

        elif ext in ['.xls', '.xlsx', '.xlsm']:
            # Nécessite openpyxl ou xlrd
            xls = pd.ExcelFile(path)
            stats["sheet_names"] = xls.sheet_names
            stats["sheet_count"] = len(xls.sheet_names)
            
            # On charge la première feuille pour les stats colonnes/preview
            if stats["sheet_names"]:
                df = pd.read_excel(xls, sheet_name=0, nrows=20)
                # Count approximatif (juste la sheet 1)
                stats["row_count"] = len(df) # C'est juste le sample nrows ici attention

        elif ext == '.parquet':
            df = pd.read_parquet(path)
            stats["row_count"] = len(df)
            
        elif ext == '.json':
            # Tente format 'records' ou 'lines'
            try:
                df = pd.read_json(path)
            except:
                try:
                    df = pd.read_json(path, lines=True)
                except:
                    pass
            if df is not None:
                stats["row_count"] = len(df)

        # Extraction commune si DF chargé
        if df is not None:
            stats["column_count"] = len(df.columns)
            stats["columns_list"] = list(df.columns.astype(str))
            
            # Preview propre (Markdown grid)
            stats["preview_str"] = df.head(10).to_markdown(index=False)
            
            # Si row_count n'a pas été calculé totalement (Excel/Gros CSV)
            if stats["row_count"] == 0 and len(df) > 0:
                stats["row_count"] = len(df) # Minima

    except Exception as e:
        print(f"Erreur Pandas {path}: {e}")

    return stats

# -------------------------
# --- MAIN DATA EXTRACT ---
# -------------------------

def extract_tabulardata_metadata_from_path(path: str) -> Dict[str, Any]:
    """
    Extrait les métadonnées de fichiers de données tabulaires.
    """
    meta = {
        "format_type": "Unknown",
        "row_count": 0,
        "column_count": 0,
        "columns_names": None, # String comma separated
        "delimiter": None,
        "sheet_count": 0,
        "sheet_names": None,
        "mime_detected": "application/octet-stream",
        "Exerpt_hund": None,
        "Exerpt_thou": None,
        "Exerpt_full": None
    }

    if not os.path.exists(path):
        return meta

    ext = os.path.splitext(path)[1].lower()
    
    # 1. Détection Format & Mime
    if ext == '.csv':
        meta["format_type"] = "CSV"
        meta["mime_detected"] = "text/csv"
    elif ext in ['.xls', '.xlsx', '.xlsm']:
        meta["format_type"] = "Excel"
        meta["mime_detected"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    elif ext == '.parquet':
        meta["format_type"] = "Parquet"
        meta["mime_detected"] = "application/vnd.apache.parquet"
    elif ext == '.json':
        meta["format_type"] = "JSON"
        meta["mime_detected"] = "application/json"
    elif ext == '.tsv':
        meta["format_type"] = "TSV"
        meta["mime_detected"] = "text/tab-separated-values"

    # 2. Analyse Contenu
    preview_content = ""
    
    if HAVE_PANDAS:
        p_stats = _analyze_with_pandas(path, ext)
        meta["row_count"] = p_stats["row_count"]
        meta["column_count"] = p_stats["column_count"]
        meta["columns_names"] = ", ".join(p_stats["columns_list"])
        meta["sheet_count"] = p_stats["sheet_count"]
        if p_stats["sheet_names"]:
            meta["sheet_names"] = ", ".join(p_stats["sheet_names"])
        preview_content = p_stats["preview_str"]
    
    elif meta["format_type"] in ["CSV", "TSV"]:
        # Fallback CSV standard
        c_stats = _analyze_csv_fallback(path)
        meta["row_count"] = c_stats["row_count"]
        meta["column_count"] = c_stats["column_count"]
        meta["columns_names"] = ", ".join(c_stats["columns_list"])
        meta["delimiter"] = c_stats["delimiter"]
        preview_content = c_stats["preview_str"]

    # 3. Construction des Extraits
    # Header summary
    summary = f"FORMAT: {meta['format_type']}\nSHAPE: {meta['row_count']} rows x {meta['column_count']} cols\n"
    if meta["columns_names"]:
        summary += f"COLUMNS: {meta['columns_names']}\n"
    if meta["sheet_names"]:
        summary += f"SHEETS: {meta['sheet_names']}\n"
    
    summary += "\n-- PREVIEW --\n"
    
    full_text = summary + preview_content
    
    meta["Exerpt_full"] = full_text
    meta["Exerpt_thou"] = full_text[:1000]
    meta["Exerpt_hund"] = full_text[:100]

    return meta


# -------------------------
# --- POPULATE DATABASE ---
# -------------------------

def populate_tabulardata_metadata(conn: sqlite3.Connection, file_id: int) -> None:
    """
    Lit le chemin du fichier dans la table 'file',
    extrait les métadonnées Data, et insère/met à jour :
      - file_data_metadata
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
    meta = extract_tabulardata_metadata_from_path(path)
    
    # Insertion
    cur.execute(
        """
        INSERT OR REPLACE INTO file_data_metadata (
            file_id,
            format_type,
            row_count,
            column_count,
            columns_names,
            delimiter,
            sheet_count,
            sheet_names,
            Exerpt_hund,
            Exerpt_thou,
            Exerpt_full
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            file_id,
            meta["format_type"],
            meta["row_count"],
            meta["column_count"],
            meta["columns_names"],
            meta["delimiter"],
            meta["sheet_count"],
            meta["sheet_names"],
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