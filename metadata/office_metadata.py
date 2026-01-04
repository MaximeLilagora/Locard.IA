import sqlite3
import zipfile
import datetime
import re
import os
from pathlib import Path
from xml.etree import ElementTree

# Tentative d'import pour PDF
try:
    from pypdf import PdfReader
    HAVE_PYPDF = True
except ImportError:
    HAVE_PYPDF = False

# Espaces de noms XML standards pour OpenXML (Office 2007+)
NS = {
    'cp': 'http://schemas.openxmlformats.org/package/2006/metadata/core-properties',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'dcterms': 'http://purl.org/dc/terms/',
    'dcmitype': 'http://purl.org/dc/dcmitype/',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}

def _parse_openxml_date(date_str):
    """Parse les dates ISO 8601 (ex: 2023-01-01T12:00:00Z) retournées par Office."""
    if not date_str:
        return None
    try:
        # On supprime le Z pour simplifier le parsing natif si besoin, 
        # ou on utilise fromisoformat (Python 3.7+)
        return date_str.replace('Z', '')
    except Exception:
        return date_str

def _extract_openxml_props(path):
    """
    Extrait les métadonnées des fichiers .docx, .xlsx, .pptx 
    en lisant directement le fichier docProps/core.xml dans l'archive ZIP.
    C'est beaucoup plus rapide que de charger tout le document.
    """
    meta = {}
    try:
        if not zipfile.is_zipfile(path):
            return meta

        with zipfile.ZipFile(path, 'r') as zf:
            if 'docProps/core.xml' in zf.namelist():
                with zf.open('docProps/core.xml') as f:
                    tree = ElementTree.parse(f)
                    root = tree.getroot()

                    # Mapping des champs Dublin Core vers notre dict
                    meta['title'] = root.findtext('dc:title', default=None, namespaces=NS)
                    meta['subject'] = root.findtext('dc:subject', default=None, namespaces=NS)
                    meta['author'] = root.findtext('dc:creator', default=None, namespaces=NS)
                    meta['keywords'] = root.findtext('cp:keywords', default=None, namespaces=NS)
                    meta['comments'] = root.findtext('dc:description', default=None, namespaces=NS)
                    meta['last_modified_by'] = root.findtext('cp:lastModifiedBy', default=None, namespaces=NS)
                    meta['revision_number'] = root.findtext('cp:revision', default=None, namespaces=NS)
                    
                    created = root.findtext('dcterms:created', default=None, namespaces=NS)
                    meta['created_date'] = _parse_openxml_date(created)
                    
                    modified = root.findtext('dcterms:modified', default=None, namespaces=NS)
                    meta['modified_date'] = _parse_openxml_date(modified)

            # Pour les stats étendues (pages, mots), c'est dans docProps/app.xml
            # C'est optionnel, on le tente
            if 'docProps/app.xml' in zf.namelist():
                with zf.open('docProps/app.xml') as f:
                    # Le namespace est différent pour app.xml souvent
                    # On fait une lecture brute simple pour éviter les soucis de NS changeants
                    content = f.read().decode('utf-8', errors='ignore')
                    
                    # Regex simple pour trouver <Pages>3</Pages> ou <Words>200</Words>
                    # C'est "dirty" mais robuste pour des XML simples sans parser lourd
                    pages = re.search(r'<Pages>(\d+)</Pages>', content)
                    words = re.search(r'<Words>(\d+)</Words>', content)
                    app_name = re.search(r'<Application>(.*?)</Application>', content)
                    
                    if pages: meta['page_count'] = int(pages.group(1))
                    if words: meta['word_count'] = int(words.group(1))
                    if app_name: meta['application_name'] = app_name.group(1)

    except Exception as e:
        print(f"Erreur lecture OpenXML {path}: {e}")
    
    return meta

def _extract_pdf_props(path):
    """Extrait les métadonnées PDF via pypdf."""
    meta = {}
    if not HAVE_PYPDF:
        return meta

    try:
        reader = PdfReader(path)
        info = reader.metadata
        
        if info:
            meta['title'] = info.title
            meta['author'] = info.author
            meta['subject'] = info.subject
            meta['application_name'] = info.creator or info.producer
            
            # Dates PDF format: D:20230101120000+02'00'
            # On stocke brut pour l'instant ou on nettoie
            c_date = info.creation_date
            if c_date:
                meta['created_date'] = str(c_date).replace('D:', '')
            
            m_date = info.modification_date
            if m_date:
                meta['modified_date'] = str(m_date).replace('D:', '')

        # Compte de pages
        try:
            meta['page_count'] = len(reader.pages)
        except:
            pass
            
    except Exception as e:
        print(f"Erreur lecture PDF {path}: {e}")

    return meta

def extract_office_metadata_from_path(path):
    """
    Fonction principale de dispatch.
    """
    ext = Path(path).suffix.lower()
    
    meta_defaults = {
        "author": None,
        "last_modified_by": None,
        "title": None,
        "subject": None,
        "keywords": None,
        "comments": None,
        "created_date": None,
        "modified_date": None,
        "page_count": None,
        "word_count": None,
        "revision_number": None,
        "application_name": None
    }
    
    extracted = {}
    
    if ext in ['.docx', '.xlsx', '.pptx', '.docm', '.xlsm', '.pptm']:
        extracted = _extract_openxml_props(path)
    elif ext == '.pdf':
        extracted = _extract_pdf_props(path)
    
    # Merge avec les défauts
    result = meta_defaults.copy()
    result.update(extracted)
    return result

def populate_office_metadata(conn: sqlite3.Connection, file_id: int) -> None:
    """
    Récupère le chemin, extrait les infos et remplit la table file_office_metadata.
    """
    cur = conn.cursor()
    
    row = cur.execute("SELECT path FROM file WHERE id = ?", (file_id,)).fetchone()
    if not row:
        return
        
    path = row[0]
    meta = extract_office_metadata_from_path(path)
    
    # On vérifie si on a trouvé au moins une donnée pertinente pour ne pas spammer la table
    # Si tout est None, on n'insère rien (ou on insère quand même selon la politique voulue)
    if not any(meta.values()):
        return

    cur.execute("""
        INSERT OR REPLACE INTO file_office_metadata (
            file_id,
            author,
            last_modified_by,
            title,
            subject,
            keywords,
            comments,
            created_date,
            modified_date,
            page_count,
            word_count,
            revision_number,
            application_name
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, (
        file_id,
        meta['author'],
        meta['last_modified_by'],
        meta['title'],
        meta['subject'],
        meta['keywords'],
        meta['comments'],
        meta['created_date'],
        meta['modified_date'],
        meta['page_count'],
        meta['word_count'],
        meta['revision_number'],
        meta['application_name']
    ))
    
    conn.commit()