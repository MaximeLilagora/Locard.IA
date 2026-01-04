# metadata/exe_metadata.py
import sqlite3
import os
import datetime
from typing import Dict, Any, List, Optional

try:
    import pefile
    HAVE_PEFILE = True
except ImportError:
    HAVE_PEFILE = False

# ----------------------------
# --- HELPERS ---
# ----------------------------

def _get_timestamp(pe) -> Optional[str]:
    """Récupère la date de compilation depuis l'en-tête PE."""
    try:
        ts = pe.FILE_HEADER.TimeDateStamp
        return datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return None

def _get_machine_type(pe) -> str:
    """Traduit le code machine en architecture lisible."""
    try:
        machine = pe.FILE_HEADER.Machine
        # Constantes communes
        if machine == 0x014c: return "x86 (32-bit)"
        if machine == 0x8664: return "x64 (64-bit)"
        if machine == 0x0200: return "Intel Itanium"
        if machine == 0xaa64: return "ARM64"
        return f"Unknown (0x{machine:x})"
    except Exception:
        return "Unknown"

def _get_subsystem(pe) -> str:
    """Traduit le code subsystem (GUI vs Console, etc.)."""
    try:
        sub = pe.OPTIONAL_HEADER.Subsystem
        if sub == 2: return "GUI (Windows)"
        if sub == 3: return "Console (CUI)"
        if sub == 1: return "Native/Driver"
        if sub == 7: return "POSIX"
        if sub == 9: return "Windows CE"
        if sub == 10: return "EFI Application"
        if sub == 11: return "EFI Boot Service Driver"
        if sub == 12: return "EFI Runtime Driver"
        return f"Other ({sub})"
    except Exception:
        return "Unknown"

def _is_signed(pe) -> int:
    """Vérifie sommairement la présence d'une signature numérique (Security Directory)."""
    try:
        # IMAGE_DIRECTORY_ENTRY_SECURITY is index 4
        directory = pe.OPTIONAL_HEADER.DATA_DIRECTORY[4]
        return 1 if directory.VirtualAddress != 0 and directory.Size > 0 else 0
    except Exception:
        return 0

# -------------------------
# --- MAIN DATA EXTRACT ---
# -------------------------

def extract_exe_metadata_from_path(path: str) -> Dict[str, Any]:
    """
    Extrait les métadonnées d'un fichier PE (Portable Executable).
    """
    meta = {
        "architecture": "Unknown",
        "compile_timestamp": None,
        "entry_point": None,
        "subsystem": "Unknown",
        "is_signed": 0,
        "section_count": 0,
        "import_count": 0,
        "export_count": 0,
        "mime_detected": "application/x-dosexec",
        "Exerpt_hund": None,
        "Exerpt_thou": None,
        "Exerpt_full": None
    }

    if not HAVE_PEFILE:
        return meta
    
    if not os.path.exists(path):
        return meta

    try:
        pe = pefile.PE(path, fast_load=True) # fast_load évite de parser tout le binaire immédiatement
        
        # 1. Headers de base
        meta["architecture"] = _get_machine_type(pe)
        meta["compile_timestamp"] = _get_timestamp(pe)
        meta["section_count"] = pe.FILE_HEADER.NumberOfSections
        
        # 2. Optional Header
        if hasattr(pe, 'OPTIONAL_HEADER'):
            meta["entry_point"] = hex(pe.OPTIONAL_HEADER.AddressOfEntryPoint)
            meta["subsystem"] = _get_subsystem(pe)
            meta["is_signed"] = _is_signed(pe)
        
        # 3. Parsing complet pour Imports/Exports (nécessite le chargement des data directories)
        pe.parse_data_directories()
        
        dll_list = []
        import_functions_sample = []
        
        # Imports
        if hasattr(pe, 'DIRECTORY_ENTRY_IMPORT'):
            for entry in pe.DIRECTORY_ENTRY_IMPORT:
                dll_name = entry.dll.decode('utf-8', errors='ignore')
                dll_list.append(dll_name)
                
                # On compte juste les fonctions
                meta["import_count"] += len(entry.imports)
                
                # On garde quelques noms de fonctions pour l'extrait (limité pour éviter surcharge)
                for imp in entry.imports[:3]: # Max 3 par DLL pour l'échantillon
                    if imp.name:
                        import_functions_sample.append(f"{dll_name}:{imp.name.decode('utf-8', errors='ignore')}")
        
        # Exports
        exports_list = []
        if hasattr(pe, 'DIRECTORY_ENTRY_EXPORT'):
            meta["export_count"] = len(pe.DIRECTORY_ENTRY_EXPORT.symbols)
            for exp in pe.DIRECTORY_ENTRY_EXPORT.symbols[:20]: # Max 20 exports dans l'extrait
                if exp.name:
                    exports_list.append(exp.name.decode('utf-8', errors='ignore'))

        # Sections
        sections_info = []
        for section in pe.sections:
            sec_name = section.Name.decode('utf-8', errors='ignore').strip('\x00')
            sections_info.append(f"{sec_name} ({hex(section.SizeOfRawData)})")

        pe.close()

        # 4. Construction des Extraits (Excerpts)
        # Format "Technical Summary"
        lines = []
        lines.append(f"ARCH: {meta['architecture']}")
        lines.append(f"COMPILED: {meta['compile_timestamp']}")
        lines.append(f"SUBSYSTEM: {meta['subsystem']}")
        lines.append(f"SIGNED: {'Yes' if meta['is_signed'] else 'No'}")
        lines.append(f"ENTRY_POINT: {meta['entry_point']}")
        lines.append("")
        
        if sections_info:
            lines.append("SECTIONS:")
            lines.append(", ".join(sections_info))
            lines.append("")

        if dll_list:
            lines.append("IMPORTED DLLs:")
            lines.append(", ".join(dll_list))
            lines.append("")

        if exports_list:
            lines.append("EXPORTS (Sample):")
            lines.append(", ".join(exports_list))
            lines.append("")

        # Pour Exerpt_full, on pourrait ajouter les fonctions importées si besoin, 
        # mais on reste sur le résumé lisible.
        full_text = "\n".join(lines)
        
        meta["Exerpt_full"] = full_text
        meta["Exerpt_thou"] = full_text[:1000]
        meta["Exerpt_hund"] = full_text[:100]

    except pefile.PEFormatError:
        print(f"Fichier PE invalide : {path}")
    except Exception as e:
        print(f"Erreur extraction EXE {path}: {e}")

    return meta


# -------------------------
# --- POPULATE DATABASE ---
# -------------------------

def populate_exe_metadata(conn: sqlite3.Connection, file_id: int) -> None:
    """
    Lit le chemin du fichier dans la table 'file',
    extrait les métadonnées EXE, et insère/met à jour :
      - file_exe_metadata
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
    meta = extract_exe_metadata_from_path(path)
    
    # Insertion
    cur.execute(
        """
        INSERT OR REPLACE INTO file_exe_metadata (
            file_id,
            architecture,
            compile_timestamp,
            entry_point,
            subsystem,
            is_signed,
            section_count,
            import_count,
            export_count,
            Exerpt_hund,
            Exerpt_thou,
            Exerpt_full
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            file_id,
            meta["architecture"],
            meta["compile_timestamp"],
            meta["entry_point"],
            meta["subsystem"],
            meta["is_signed"],
            meta["section_count"],
            meta["import_count"],
            meta["export_count"],
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