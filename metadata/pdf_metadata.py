# metadata/pdf_metadata.py
import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
import datetime

try:
    from pypdf import PdfReader
    from pypdf.generic import NameObject, IndirectObject
    HAVE_PYPDF = True
except ImportError:
    HAVE_PYPDF = False

# ----------------------------
# --- HELPERS ---
# ----------------------------

def _parse_pdf_date(date_str: str) -> Optional[str]:
    """
    Convertit une date PDF (ex: "D:20230101120000+02'00'") en format ISO 8601.
    """
    if not date_str:
        return None
    
    # Nettoyage basique
    clean = date_str.replace("D:", "").replace("'", "")
    
    # Format minimum YYYY
    if len(clean) < 4:
        return None
        
    try:
        year = clean[0:4]
        month = clean[4:6] if len(clean) >= 6 else "01"
        day = clean[6:8] if len(clean) >= 8 else "01"
        hour = clean[8:10] if len(clean) >= 10 else "00"
        minute = clean[10:12] if len(clean) >= 12 else "00"
        second = clean[12:14] if len(clean) >= 14 else "00"
        
        # Gestion basique du timezone (souvent à la fin)
        # Pour faire simple on renvoie du "naïf" ou on garde la string ISO basique
        return f"{year}-{month}-{day}T{hour}:{minute}:{second}"
    except Exception:
        return None

def _check_resources(page, key_name: str) -> bool:
    """Vérifie la présence de ressources (XObject pour images, etc.) dans une page."""
    try:
        if '/Resources' in page and key_name in page['/Resources']:
            return True
    except Exception:
        pass
    return False

# -------------------------
# --- MAIN DATA EXTRACT ---
# -------------------------

def extract_pdf_metadata_from_path(path: str) -> Dict[str, Any]:
    """
    Extrait les métadonnées techniques, structurelles et le contenu textuel d'un PDF.
    """
    meta = {
        # Structure
        "page_count": 0,
        "has_text": 0,
        "has_images": 0,
        "has_forms": 0,
        "has_signatures": 0,
        "is_encrypted": 0,

        # Metadata standard
        "title": None,
        "author": None,
        "subject": None,
        "keywords": None,
        "creator": None,
        "producer": None,
        "language": None,
        "created_at": None,
        "modified_at": None,
        "pdf_version": None,
        "pdf_conformance": None, # Difficile à extraire de manière fiable sans libs lourdes

        # Text Content (Excerpts)
        "Exerpt_hund": None,
        "Exerpt_thou": None,
        "Exerpt_full": None,
        
        # Tech
        "is_blurred": 0,      # Nécessite analyse image (CV2), laissé à 0 ici
        "is_llavaocr_req": 0, # Sera déterminé par la logique métier si has_text=0 et has_images=1
        "mime_detected": "application/pdf"
    }

    if not HAVE_PYPDF:
        return meta

    try:
        reader = PdfReader(path)
        
        # 1. Encryption
        if reader.is_encrypted:
            meta["is_encrypted"] = 1
            # Tentative de décryptage avec mot de passe vide (lecture standard)
            try:
                reader.decrypt("")
            except:
                # Si on ne peut pas lire, on s'arrête là pour le contenu
                return meta

        # 2. Informations générales
        info = reader.metadata
        if info:
            meta["title"] = info.title
            meta["author"] = info.author
            meta["subject"] = info.subject
            meta["creator"] = info.creator
            meta["producer"] = info.producer
            
            # Keywords peut être une liste ou une chaîne
            kw = info.get("/Keywords")
            if isinstance(kw, str):
                meta["keywords"] = kw
            
            meta["created_at"] = _parse_pdf_date(info.get("/CreationDate"))
            meta["modified_at"] = _parse_pdf_date(info.get("/ModDate"))

        # Version PDF
        try:
            # pdf_header ressemble à "%PDF-1.6"
            header = reader.pdf_header
            if header and "-" in header:
                meta["pdf_version"] = header.split("-")[-1].strip()
        except:
            pass

        # 3. Structure et Pages
        try:
            num_pages = len(reader.pages)
            meta["page_count"] = num_pages
        except:
            num_pages = 0

        # Extraction de texte et détection d'images (Scan rapide sur les 10 premières pages max)
        full_text = []
        has_images = False
        has_text = False
        
        scan_limit = min(num_pages, 20) # On limite l'analyse profonde pour la perf

        for i in range(scan_limit):
            try:
                page = reader.pages[i]
                
                # Texte
                text = page.extract_text()
                if text and len(text.strip()) > 0:
                    full_text.append(text)
                    has_text = True
                
                # Images (Heuristique rapide via Resources -> XObject)
                if not has_images:
                    if '/Resources' in page and '/XObject' in page['/Resources']:
                        xobj = page['/Resources']['/XObject'].get_object()
                        for obj in xobj:
                            if xobj[obj]['/Subtype'] == '/Image':
                                has_images = True
                                break
            except Exception:
                continue

        meta["has_text"] = 1 if has_text else 0
        meta["has_images"] = 1 if has_images else 0
        
        # Forms (AcroForm)
        if reader.get_fields():
            meta["has_forms"] = 1

        # Signatures (recherche basique dans la racine)
        # Note: c'est une heuristique, la validation de signature est complexe
        if "/AcroForm" in reader.trailer["/Root"]:
            acroform = reader.trailer["/Root"]["/AcroForm"]
            if "/SigFlags" in acroform:
                sig_flags = acroform["/SigFlags"]
                if sig_flags and (sig_flags & 1) or (sig_flags & 2): # Signature or AppendOnly
                    meta["has_signatures"] = 1
                    
        # 4. Remplissage des Extraits
        all_text_content = "\n".join(full_text)
        if all_text_content:
            meta["Exerpt_full"] = all_text_content
            meta["Exerpt_thou"] = all_text_content[:1000]
            meta["Exerpt_hund"] = all_text_content[:100]
        
        # Logique simple pour demande d'OCR : pas de texte mais des images
        if meta["has_text"] == 0 and meta["has_images"] == 1:
            meta["is_llavaocr_req"] = 1

    except Exception as e:
        print(f"Erreur extraction PDF {path}: {e}")

    return meta


# -------------------------
# --- POPULATE DATABASE ---
# -------------------------

def populate_pdf_metadata(conn: sqlite3.Connection, file_id: int) -> None:
    """
    Lit le chemin du fichier dans la table 'file',
    extrait les métadonnées PDF, et insère/met à jour :
      - file_pdf_metadata
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
    meta = extract_pdf_metadata_from_path(path)
    
    # Si lecture échouée totalement (fichier corrompu ou non PDF), on passe
    # On vérifie si on a au moins réussi à ouvrir le PDF (page_count >= 0 ou is_encrypted)
    # Si page_count est 0 et non chiffré, c'est peut-être un fichier vide ou erreur.
    
    cur.execute(
        """
        INSERT OR REPLACE INTO file_pdf_metadata (
            file_id,
            page_count,
            has_text,
            has_images,
            has_forms,
            has_signatures,
            is_encrypted,
            title,
            author,
            subject,
            keywords,
            creator,
            producer,
            language,
            created_at,
            modified_at,
            pdf_version,
            pdf_conformance,
            is_blurred,
            is_llavaocr_req,
            Exerpt_hund,
            Exerpt_thou,
            Exerpt_full
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?
        )
        """,
        (
            file_id,
            meta["page_count"],
            meta["has_text"],
            meta["has_images"],
            meta["has_forms"],
            meta["has_signatures"],
            meta["is_encrypted"],
            meta["title"],
            meta["author"],
            meta["subject"],
            meta["keywords"],
            meta["creator"],
            meta["producer"],
            meta["language"],
            meta["created_at"],
            meta["modified_at"],
            meta["pdf_version"],
            meta["pdf_conformance"],
            meta["is_blurred"],
            meta["is_llavaocr_req"],
            meta["Exerpt_hund"],
            meta["Exerpt_thou"],
            meta["Exerpt_full"]
        ),
    )

    # Mise à jour du MIME confirmé
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