# metadata/archive_metadata.py
import sqlite3
import os
import zipfile
import tarfile
from typing import Dict, Any, List
import datetime

# -------------------------
# --- HELPERS ---
# -------------------------

def _analyze_zip(path: str) -> Dict[str, Any]:
    """Analyse spécifique pour les fichiers ZIP."""
    stats = {
        "file_count": 0,
        "folder_count": 0,
        "total_uncompressed_size": 0,
        "is_encrypted": 0,
        "comment": None,
        "file_list": []
    }
    
    try:
        with zipfile.ZipFile(path, 'r') as zf:
            # Commentaire global
            if zf.comment:
                try:
                    stats["comment"] = zf.comment.decode('utf-8', errors='replace')
                except:
                    pass

            for info in zf.infolist():
                # Détection dossier vs fichier
                if info.is_dir():
                    stats["folder_count"] += 1
                else:
                    stats["file_count"] += 1
                    stats["total_uncompressed_size"] += info.file_size
                
                # Détection chiffrement (Bit 0 de flag_bits)
                if info.flag_bits & 0x1:
                    stats["is_encrypted"] = 1
                
                stats["file_list"].append(info.filename)
                
    except zipfile.BadZipFile:
        print(f"Erreur: Fichier ZIP invalide {path}")
    except Exception as e:
        print(f"Erreur lecture ZIP {path}: {e}")
        
    return stats

def _analyze_tar(path: str) -> Dict[str, Any]:
    """Analyse spécifique pour les fichiers TAR (et .tar.gz, .tgz)."""
    stats = {
        "file_count": 0,
        "folder_count": 0,
        "total_uncompressed_size": 0,
        "is_encrypted": 0, # Tar ne supporte pas le chiffrement natif par fichier
        "comment": None,
        "file_list": []
    }
    
    try:
        # tarfile gère automatiquement la compression (gz, bz2) si mode='r:*'
        with tarfile.open(path, 'r:*') as tf:
            for member in tf:
                if member.isdir():
                    stats["folder_count"] += 1
                elif member.isfile():
                    stats["file_count"] += 1
                    stats["total_uncompressed_size"] += member.size
                
                stats["file_list"].append(member.name)
                
    except tarfile.ReadError:
        print(f"Erreur: Fichier TAR invalide ou illisible {path}")
    except Exception as e:
        print(f"Erreur lecture TAR {path}: {e}")

    return stats

# -------------------------
# --- MAIN DATA EXTRACT ---
# -------------------------

def extract_archive_metadata_from_path(path: str) -> Dict[str, Any]:
    """
    Extrait les métadonnées d'une archive (Zip, Tar, Gzip).
    """
    meta = {
        "file_count": 0,
        "folder_count": 0,
        "total_uncompressed_size": 0,
        "compression_ratio": 0.0,
        "is_encrypted": 0,
        "mime_detected": "application/octet-stream",
        "Exerpt_hund": None,
        "Exerpt_thou": None,
        "Exerpt_full": None
    }

    if not os.path.exists(path):
        return meta
        
    ext = os.path.splitext(path)[1].lower()
    file_size_on_disk = os.path.getsize(path)
    
    # Choix du moteur d'analyse
    internal_stats = None
    
    if ext == '.zip':
        meta["mime_detected"] = "application/zip"
        internal_stats = _analyze_zip(path)
    elif ext in ['.tar', '.gz', '.tgz', '.bz2']:
        # Note: .gz simple est techniquement un flux compressé, mais souvent traité comme archive mono-fichier ou tarball
        if path.endswith('.tar') or '.tar.' in path or path.endswith('.tgz'):
             meta["mime_detected"] = "application/x-tar"
        else:
             meta["mime_detected"] = "application/gzip"
        internal_stats = _analyze_tar(path)
    else:
        # Tenter Zip par défaut si extension inconnue mais structure suspectée ? 
        # Pour l'instant on ignore pour éviter les faux positifs.
        pass

    if internal_stats:
        meta["file_count"] = internal_stats["file_count"]
        meta["folder_count"] = internal_stats["folder_count"]
        meta["total_uncompressed_size"] = internal_stats["total_uncompressed_size"]
        meta["is_encrypted"] = internal_stats["is_encrypted"]
        
        # Calcul ratio
        if file_size_on_disk > 0:
            # Ratio > 1 signifie compression efficace (ex: 100Mo -> 50Mo = ratio 2.0)
            meta["compression_ratio"] = round(internal_stats["total_uncompressed_size"] / file_size_on_disk, 2)
        
        # Génération des extraits basés sur la liste des fichiers
        # C'est souvent l'info la plus pertinente pour la recherche textuelle dans une archive
        full_list_str = "\n".join(internal_stats["file_list"])
        
        # Ajout du commentaire s'il existe
        if internal_stats.get("comment"):
            full_list_str = f"COMMENT: {internal_stats['comment']}\n\nFILES:\n{full_list_str}"

        if full_list_str:
            meta["Exerpt_full"] = full_list_str
            meta["Exerpt_thou"] = full_list_str[:1000]
            meta["Exerpt_hund"] = full_list_str[:100]

    return meta


# -------------------------
# --- POPULATE DATABASE ---
# -------------------------

def populate_archive_metadata(conn: sqlite3.Connection, file_id: int) -> None:
    """
    Lit le chemin du fichier dans la table 'file',
    extrait les métadonnées Archive, et insère/met à jour :
      - file_archive_metadata
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
    meta = extract_archive_metadata_from_path(path)
    
    cur.execute(
        """
        INSERT OR REPLACE INTO file_archive_metadata (
            file_id,
            file_count,
            folder_count,
            total_uncompressed_size,
            compression_ratio,
            is_encrypted,
            Exerpt_hund,
            Exerpt_thou,
            Exerpt_full
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            file_id,
            meta["file_count"],
            meta["folder_count"],
            meta["total_uncompressed_size"],
            meta["compression_ratio"],
            meta["is_encrypted"],
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