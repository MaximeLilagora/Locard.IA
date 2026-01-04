# metadata/disk_img_metadata.py
import sqlite3
import os
import struct
from typing import Dict, Any, List, Optional

# Tentative d'import de pycdlib pour le parsing ISO avancé
try:
    import pycdlib
    HAVE_PYCDLIB = True
except ImportError:
    HAVE_PYCDLIB = False

# ----------------------------
# --- HELPERS ---
# ----------------------------

def _detect_disk_format(path: str) -> str:
    """
    Tente de détecter le format de l'image disque via les Magic Bytes.
    """
    try:
        with open(path, 'rb') as f:
            header = f.read(512)
            
            # ISO 9660 (Check standard volumes descriptors at offset 32768 usually, 
            # but usually start with 00 or system area. 
            # Pycdlib is better for ISO, here we check specific magic bytes for others)
            
            # VHD (Connectix) - Cookie "conectix" often at end of file, but sometimes header at beginning
            # VMDK - Starts with "KDMV"
            if header.startswith(b'KDMV'):
                return "vmdk"
            
            # QCOW2
            if header.startswith(b'QFI\xfb'):
                return "qcow2"
            
            # DMG (Universal Disk Image Format) - Often has 'koly' block at end, but magic 'UDIF' exists
            # Checking generic signature is harder without parsing XML.
            
            # VDI (VirtualBox)
            if header[64:68] == b'\x7f\x10\xda\xbe':
                return "vdi"

    except Exception:
        pass
    
    # Fallback sur l'extension si pas de magic byte évident détecté
    ext = os.path.splitext(path)[1].lower()
    if ext == '.iso': return "iso"
    if ext == '.img': return "raw_img"
    if ext == '.vhd': return "vhd"
    if ext == '.vhdx': return "vhdx"
    if ext == '.dmg': return "dmg"
    
    return "unknown"

def _analyze_iso(path: str) -> Dict[str, Any]:
    """Analyse spécifique pour les fichiers ISO 9660 / UDF."""
    stats = {
        "volume_label": None,
        "file_system": "Unknown",
        "file_count": 0,
        "is_bootable": 0,
        "file_list": []
    }
    
    if not HAVE_PYCDLIB:
        return stats

    iso = pycdlib.PyCdlib()
    try:
        iso.open(path)
        
        # Détection type système de fichiers
        if iso.has_udf():
            stats["file_system"] = "UDF"
            # Priorité UDF pour le label
            try:
                stats["volume_label"] = iso.udf_get_vol_ident().decode('utf-8', errors='replace')
            except:
                pass
        elif iso.has_joliet():
            stats["file_system"] = "ISO9660+Joliet"
            try:
                stats["volume_label"] = iso.joliet_get_volume_id().decode('utf-8', errors='replace')
            except:
                pass
        elif iso.has_rock_ridge():
            stats["file_system"] = "ISO9660+RockRidge"
            try:
                stats["volume_label"] = iso.get_volume_id().decode('utf-8', errors='replace')
            except:
                pass
        else:
            stats["file_system"] = "ISO9660"
            try:
                stats["volume_label"] = iso.get_volume_id().decode('utf-8', errors='replace')
            except:
                pass

        # Check Bootable (El Torito)
        if iso.has_eltorito():
            stats["is_bootable"] = 1

        # Listing des fichiers (Walk)
        # On utilise une méthode générique compatible Joliet/RR/ISO standard
        # pycdlib walk renvoie (dir_path, dir_names, file_names)
        try:
            for root, dirs, files in iso.walk(iso_path='/'):
                for f in files:
                    stats["file_count"] += 1
                    # Construit le chemin complet
                    full_path = f"{root}/{f}".replace('//', '/')
                    stats["file_list"].append(full_path)
        except Exception:
            # Parfois le walk échoue sur des ISOs mal formés
            pass

    except Exception as e:
        print(f"Erreur analyse ISO {path}: {e}")
    finally:
        try:
            iso.close()
        except:
            pass
            
    return stats

# -------------------------
# --- MAIN DATA EXTRACT ---
# -------------------------

def extract_disk_image_metadata_from_path(path: str) -> Dict[str, Any]:
    """
    Extrait les métadonnées d'une image disque.
    """
    meta = {
        "format_type": "unknown",
        "volume_label": None,
        "file_system": None,
        "file_count": 0,
        "total_size_bytes": 0,
        "is_bootable": 0,
        "mime_detected": "application/octet-stream",
        "Exerpt_hund": None,
        "Exerpt_thou": None,
        "Exerpt_full": None
    }

    if not os.path.exists(path):
        return meta

    try:
        meta["total_size_bytes"] = os.path.getsize(path)
        
        # 1. Identification Format
        detected_fmt = _detect_disk_format(path)
        meta["format_type"] = detected_fmt
        
        # Mapping Mime Type basique
        mime_map = {
            "iso": "application/x-iso9660-image",
            "vmdk": "application/x-virtualbox-vmdk",
            "vdi": "application/x-virtualbox-vdi",
            "qcow2": "application/x-qemu-disk",
            "dmg": "application/x-apple-diskimage",
            "vhd": "application/x-vhd",
            "raw_img": "application/x-raw-disk-image"
        }
        meta["mime_detected"] = mime_map.get(detected_fmt, "application/octet-stream")

        # 2. Analyse Contenu (Spécifique ISO pour l'instant)
        if detected_fmt == "iso":
            iso_stats = _analyze_iso(path)
            meta["volume_label"] = iso_stats["volume_label"]
            meta["file_system"] = iso_stats["file_system"]
            meta["file_count"] = iso_stats["file_count"]
            meta["is_bootable"] = iso_stats["is_bootable"]
            
            # Génération Extraits (Liste des fichiers)
            if iso_stats["file_list"]:
                full_list_str = "\n".join(iso_stats["file_list"])
                
                header_info = f"LABEL: {meta['volume_label'] or 'N/A'}\nFS: {meta['file_system']}\nBOOTABLE: {meta['is_bootable']}\n\nFILES:\n"
                full_content = header_info + full_list_str
                
                meta["Exerpt_full"] = full_content
                meta["Exerpt_thou"] = full_content[:1000]
                meta["Exerpt_hund"] = full_content[:100]
        
        else:
            # Pour les autres formats, on met juste les infos de base dans l'extrait pour l'instant
            # car l'extraction de fichier demande de monter le disque (complexe en pur python)
            info_str = f"FORMAT: {detected_fmt.upper()}\nSIZE: {meta['total_size_bytes']} bytes"
            meta["Exerpt_hund"] = info_str
            meta["Exerpt_thou"] = info_str
            meta["Exerpt_full"] = info_str

    except Exception as e:
        print(f"Erreur extraction DiskImg {path}: {e}")

    return meta


# -------------------------
# --- POPULATE DATABASE ---
# -------------------------

def populate_disk_image_metadata(conn: sqlite3.Connection, file_id: int) -> None:
    """
    Lit le chemin du fichier dans la table 'file',
    extrait les métadonnées Disk Image, et insère/met à jour :
      - file_disk_image_metadata
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
    meta = extract_disk_image_metadata_from_path(path)
    
    cur.execute(
        """
        INSERT OR REPLACE INTO file_disk_image_metadata (
            file_id,
            format_type,
            volume_label,
            file_system,
            file_count,
            total_size_bytes,
            is_bootable,
            Exerpt_hund,
            Exerpt_thou,
            Exerpt_full
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            file_id,
            meta["format_type"],
            meta["volume_label"],
            meta["file_system"],
            meta["file_count"],
            meta["total_size_bytes"],
            meta["is_bootable"],
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