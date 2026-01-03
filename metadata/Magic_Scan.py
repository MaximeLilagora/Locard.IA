#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import zipfile
import sqlite3
from pathlib import Path
import mimetypes

# =========================
# python-magic (libmagic)
# =========================
try:
    import magic  # paquet python-magic
    HAVE_MAGIC = True
except ImportError:
    magic = None
    HAVE_MAGIC = False

# =========================
# Placeholder chemin fichier
# =========================
FilePathScan = r"FilePathScan"  # indiquer ici le chemin du fichier à scan

# =========================
# Variable de résultat
# =========================
TrueExtension = None  # sera rempli après détection (par ex. ".png", ".pdf", ".zip", etc.)

# =========================
# Table MIME → extension
# =========================
MIME_TO_EXT = {
    # Images
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/bmp": ".bmp",
    "image/webp": ".webp",
    "image/tiff": ".tif",
    "image/x-icon": ".ico",
    "image/vnd.microsoft.icon": ".ico",
    "image/vnd.adobe.photoshop": ".psd",

    # Audio
    "audio/mpeg": ".mp3",
    "audio/x-wav": ".wav",
    "audio/wav": ".wav",
    "audio/flac": ".flac",
    "audio/ogg": ".ogg",
    "audio/opus": ".opus",

    # Vidéo
    "video/mp4": ".mp4",
    "video/x-msvideo": ".avi",
    "video/x-matroska": ".mkv",
    "video/webm": ".webm",
    "video/3gpp": ".3gp",

    # Archives / compressés
    "application/zip": ".zip",
    "application/x-7z-compressed": ".7z",
    "application/x-rar-compressed": ".rar",
    "application/gzip": ".gz",
    "application/x-gzip": ".gz",
    "application/x-bzip2": ".bz2",
    "application/x-xz": ".xz",
    "application/x-lz4": ".lz4",

    # Documents
    "application/pdf": ".pdf",
    "application/postscript": ".ps",
    "application/rtf": ".rtf",
    "text/rtf": ".rtf",
    "text/plain": ".txt",
    "text/html": ".html",
    "application/xhtml+xml": ".html",
    "application/xml": ".xml",
    "text/xml": ".xml",
    "application/json": ".json",

    # Office Open XML
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",

    # OpenDocument
    "application/vnd.oasis.opendocument.text": ".odt",
    "application/vnd.oasis.opendocument.spreadsheet": ".ods",
    "application/vnd.oasis.opendocument.presentation": ".odp",

    # EPUB
    "application/epub+zip": ".epub",

    # Bases
    "application/vnd.sqlite3": ".sqlite",
    "application/x-sqlite3": ".sqlite",

    # Polices
    "font/ttf": ".ttf",
    "font/otf": ".otf",
    "font/woff": ".woff",
    "font/woff2": ".woff2",

    # Executables / binaires
    "application/x-dosexec": ".exe",
    "application/x-executable": "",      # ELF, extension variable
    "application/x-mach-binary": "",     # Mach-O
}


def mime_to_extension(mime: str | None) -> str | None:
    """
    Retourne une extension probable à partir d'un type MIME.
    """
    if not mime:
        return None

    # 1) Table custom
    ext = MIME_TO_EXT.get(mime)
    if ext is not None:
        return ext

    # 2) Fallback mimetypes
    ext = mimetypes.guess_extension(mime, strict=False)
    if not ext:
        return None

    # Normalisation de quelques cas bizarres (.jpe → .jpg, etc.)
    if ext in (".jpe",):
        return ".jpg"

    return ext


def detect_true_extension_for_path(path: str):
    """
    Retourne (ext, desc) pour un fichier donné.
    ext peut être None si le format est inconnu.
    """
    data = read_file_start(path)
    ext, desc = detect_file_type(data, path_for_containers=path)
    return ext, desc


def run_magic_numbers_on_db(
    db_path: str,
    base_dir: str | None = None,
    only_missing: bool = True,
    progress_callback=None,
):
    """
    Parcourt la table `file` d'une base SQLite et remplit la colonne true_extension
    en utilisant python-magic + magic numbers (fallback).

    - db_path : chemin vers working_DB/project_index.db
    - base_dir : répertoire racine pour résoudre les chemins relatifs (optionnel)
    - only_missing : si True, ne traite que les lignes où true_extension est NULL ou vide
    - progress_callback : fonction appelée à chaque fichier traité pour faire
      remonter les infos à Streamlit (optionnel)
    """

    db_path = Path(db_path)

    if base_dir is not None:
        base_dir = Path(base_dir)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 1) Récupération des fichiers à traiter
    if only_missing:
        cur.execute(
            """
            SELECT id, path
            FROM file
            WHERE true_extension IS NULL OR true_extension = ''
            """
        )
    else:
        cur.execute("SELECT id, path FROM file")

    rows = cur.fetchall()
    total = len(rows)

    if total == 0:
        if progress_callback:
            progress_callback(0, 0, None, None, None, "Rien à traiter.")
        conn.close()
        return

    for idx, (file_id, rel_path) in enumerate(rows, start=1):
        # Résolution du chemin : absolu ou relatif à base_dir
        if base_dir is not None and not os.path.isabs(rel_path):
            full_path = str(base_dir / rel_path)
        else:
            full_path = rel_path

        ext = None
        desc = None
        error = None

        try:
            if not os.path.isfile(full_path):
                error = f"Fichier introuvable : {full_path}"
            else:
                ext, desc = detect_true_extension_for_path(full_path)

                # Mise à jour de la base (on peut stocker ext même si None pour indiquer "inconnu")
                cur.execute(
                    "UPDATE file SET true_extension = ? WHERE id = ?",
                    (ext, file_id),
                )
        except Exception as e:
            error = str(e)

        # Callback vers l’UI (Streamlit) si fourni
        if progress_callback:
            progress_callback(idx, total, full_path, ext, desc, error)

        # On peut commit par batch, mais pour un premier jet on commit à chaque fois
        conn.commit()

    conn.close()


def read_file_start(path, max_bytes=4096):
    """Lit les premiers octets du fichier."""
    with open(path, "rb") as f:
        return f.read(max_bytes)


def detect_zip_subtype(path):
    """
    Essaie de distinguer les différents types basés sur ZIP :
    DOCX, XLSX, PPTX, ODT, ODS, ODP, EPUB, APK, JAR, WAR, etc.
    Retourne (extension, description) ou (".zip", "ZIP archive") par défaut.
    """
    try:
        with zipfile.ZipFile(path, "r") as z:
            names = z.namelist()
            lower_names = [n.lower() for n in names]
            name_set = set(lower_names)

            # Office Open XML (docx, xlsx, pptx)
            if any(n.startswith("word/") for n in lower_names):
                return ".docx", "Microsoft Word Open XML document (DOCX)"
            if any(n.startswith("xl/") for n in lower_names):
                return ".xlsx", "Microsoft Excel Open XML spreadsheet (XLSX)"
            if any(n.startswith("ppt/") for n in lower_names):
                return ".pptx", "Microsoft PowerPoint Open XML presentation (PPTX)"

            # OpenDocument (LibreOffice, OpenOffice)
            if "mimetype" in lower_names:
                try:
                    mt = z.read("mimetype").decode("ascii", "ignore").strip()
                except Exception:
                    mt = ""
                if mt == "application/vnd.oasis.opendocument.text":
                    return ".odt", "OpenDocument Text (ODT)"
                if mt == "application/vnd.oasis.opendocument.spreadsheet":
                    return ".ods", "OpenDocument Spreadsheet (ODS)"
                if mt == "application/vnd.oasis.opendocument.presentation":
                    return ".odp", "OpenDocument Presentation (ODP)"
                if mt == "application/epub+zip":
                    return ".epub", "EPUB e-book"

            # APK (Android)
            if "androidmanifest.xml" in name_set:
                return ".apk", "Android application package (APK)"

            # JAR / WAR / EAR (Java)
            if "meta-inf/manifest.mf" in name_set:
                if any(n.startswith("web-inf/") for n in lower_names):
                    return ".war", "Java Web Application Archive (WAR)"
                # Distinction JAR/EAR demande des règles supplémentaires ;
                # on se contente de JAR générique.
                return ".jar", "Java archive (JAR)"

            # Fallback ZIP
            return ".zip", "Generic ZIP archive"
    except Exception:
        # Pas un ZIP valide ou erreur de lecture → on garde ZIP générique
        return ".zip", "ZIP-like file (invalid or unreadable as ZIP)"


def detect_file_type_manual(data, path_for_containers=None):
    """
    Ancien détecteur basé sur les magic numbers.
    Utilisé comme fallback si python-magic ne sait pas.
    Retourne (extension_probable, description).
    """
    # ======================
    # 1. Archives & compression
    # ======================
    if data.startswith(b"PK\x03\x04") or data.startswith(b"PK\x05\x06") or data.startswith(b"PK\x07\x08"):
        if path_for_containers is not None:
            return detect_zip_subtype(path_for_containers)
        return ".zip", "Generic ZIP archive"

    if data.startswith(b"Rar!\x1A\x07\x00"):
        return ".rar", "RAR archive (v1.5–4.x)"
    if data.startswith(b"Rar!\x1A\x07\x01\x00"):
        return ".rar", "RAR archive (v5+)"
    if data.startswith(b"7z\xBC\xAF\x27\x1C"):
        return ".7z", "7-Zip archive"
    if data.startswith(b"\x1F\x8B\x08"):
        return ".gz", "GZIP compressed file"
    if data.startswith(b"BZh"):
        return ".bz2", "BZIP2 compressed file"
    if data.startswith(b"\xFD7zXZ\x00"):
        return ".xz", "XZ compressed file"
    if data.startswith(b"\x04\x22\x4D\x18"):
        return ".lz4", "LZ4 frame"
    if data.startswith(b"MSCF"):
        return ".cab", "Microsoft Cabinet archive"

    # tar : "ustar" à l'offset 257
    if len(data) >= 262 and data[257:262] == b"ustar":
        return ".tar", "TAR archive"

    # ======================
    # 2. Images
    # ======================
    if data.startswith(b"\xFF\xD8\xFF"):
        return ".jpg", "JPEG image"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png", "PNG image"
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return ".gif", "GIF image"
    if data.startswith(b"BM"):
        return ".bmp", "BMP bitmap image"
    if data.startswith(b"\x49\x49\x2A\x00"):
        return ".tif", "TIFF image (little-endian)"
    if data.startswith(b"\x4D\x4D\x00\x2A") or data.startswith(b"\x4D\x4D\x00\x2B"):
        return ".tif", "TIFF image (big-endian)"
    if data.startswith(b"\x00\x00\x01\x00"):
        return ".ico", "ICO icon (Windows)"
    if data.startswith(b"\x00\x00\x02\x00"):
        return ".cur", "CUR cursor (Windows)"
    if data.startswith(b"8BPS"):
        return ".psd", "Adobe Photoshop document (PSD)"

    # WEBP / RIFF images
    if data.startswith(b"RIFF") and len(data) >= 12:
        subtype = data[8:12]
        if subtype == b"WEBP":
            return ".webp", "WebP image"
        if subtype == b"WAVE":
            return ".wav", "WAVE audio (RIFF/WAVE)"
        if subtype == b"AVI ":
            return ".avi", "AVI video (RIFF/AVI)"

    # ======================
    # 3. Audio / Vidéo / multimédia
    # ======================
    if data.startswith(b"OggS"):
        # Peut être .ogg, .oga, .opus ; on reste générique
        return ".ogg", "Ogg container (Vorbis/Opus/etc.)"
    if data.startswith(b"fLaC"):
        return ".flac", "FLAC audio"
    if data.startswith(b"ID3"):
        return ".mp3", "MP3 audio (ID3v2 tag)"
    # EBML / Matroska / WebM
    if data.startswith(b"\x1A\x45\xDF\xA3"):
        return ".mkv", "Matroska/WebM container (MKV/WEBM)"
    # MP4 / ISO Base Media (ftyp à l’offset 4)
    if len(data) >= 12 and data[4:8] == b"ftyp":
        brand = data[8:12]
        if brand in (b"isom", b"mp42", b"mp41", b"MSNV"):
            return ".mp4", "MP4/ISO Base Media file"
        if brand.startswith(b"3gp"):
            return ".3gp", "3GPP media file"
        if brand in (b"qt  ",):
            return ".mov", "QuickTime movie"
        # fallback
        return ".mp4", "ISO Base Media (MP4-like)"

    # ======================
    # 4. Documents texte / bureautique / bases
    # ======================
    if data.startswith(b"%PDF-"):
        return ".pdf", "PDF document"
    if data.startswith(b"%!PS-"):
        return ".ps", "PostScript document"
    if data.startswith(b"{\\rtf"):
        return ".rtf", "RTF document"
    if data.startswith(b"SQLite format 3\x00"):
        return ".sqlite", "SQLite 3 database"

    # XML / HTML / JSON (heuristiques, pas de vraie magic number)
    prefix = data.lstrip()[:16].lower()
    if prefix.startswith(b"<?xml"):
        return ".xml", "XML text document"
    if prefix.startswith(b"<!doctype html") or prefix.startswith(b"<html"):
        return ".html", "HTML document"
    if prefix.startswith(b"{") or prefix.startswith(b"["):
        # très heuristique…
        return ".json", "JSON text (heuristic)"

    # OLE2 / Compound File Binary (DOC, XLS, PPT, VSD, MSG…)
    if data.startswith(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"):
        # Difficile de distinguer sans parser la structure ; on reste générique.
        return ".doc", "OLE2 Compound File (DOC/XLS/PPT/etc.)"

    # ======================
    # 5. Exécutables / binaires / scripts
    # ======================
    # Windows PE/COFF
    if data.startswith(b"MZ"):
        return ".exe", "PE executable or library (Windows EXE/DLL/SYS)"

    # ELF (Linux/Unix)
    if data.startswith(b"\x7FELF"):
        return "", "ELF executable or shared object (Unix/Linux)"

    # Mach-O (macOS)
    macho_magics = (
        b"\xFE\xED\xFA\xCE",  # 32-bit big-endian
        b"\xCE\xFA\xED\xFE",  # 32-bit little-endian
        b"\xFE\xED\xFA\xCF",  # 64-bit big-endian
        b"\xCF\xFA\xED\xFE",  # 64-bit little-endian
    )
    if data.startswith(macho_magics):
        return "", "Mach-O executable (macOS)"

    # Java class / Mach-O universel (CAFEBABE)
    if data.startswith(b"\xCA\xFE\xBA\xBE"):
        return ".class", "Java class file (or Mach-O fat binary)"

    # Scripts avec shebang (#!)
    if data.startswith(b"#!"):
        return ".sh", "Script with shebang (shell / Python / etc.)"

    # ======================
    # 6. Polices (fonts)
    # ======================
    if data.startswith(b"\x00\x01\x00\x00"):
        return ".ttf", "TrueType font"
    if data.startswith(b"OTTO"):
        return ".otf", "OpenType font (CFF)"
    if data.startswith(b"wOFF"):
        return ".woff", "WOFF web font"
    if data.startswith(b"wOF2"):
        return ".woff2", "WOFF2 web font"

    # ======================
    # 7. Réseaux / captures / autres
    # ======================
    if data.startswith(b"\xD4\xC3\xB2\xA1") or data.startswith(b"\xA1\xB2\xC3\xD4"):
        return ".pcap", "PCAP capture file"
    if data.startswith(b"\x0A\x0D\x0D\x0A"):
        return ".pcapng", "PCAP-NG capture file"

    # ======================
    # Fallback
    # ======================
    return None, "Unknown or unsupported format"


def detect_file_type(data, path_for_containers=None):
    """
    Détecte le type de fichier à partir des premiers octets, en combinant :
    - python-magic (libmagic) pour identifier le MIME et une description
    - tes règles custom (ZIP → DOCX/XLSX/…)
    - ton ancien détecteur par magic numbers en fallback

    Retourne (extension_probable, description).
    """

    # ======================
    # 0. D'abord : cas ZIP spéciaux à partir du header
    # (on veut pouvoir distinguer DOCX/XLSX/PPTX/ODT/EPUB/etc.)
    # ======================
    if data.startswith(b"PK\x03\x04") or data.startswith(b"PK\x05\x06") or data.startswith(b"PK\x07\x08"):
        if path_for_containers is not None:
            return detect_zip_subtype(path_for_containers)
        # Si on n'a pas le chemin, on laissera python-magic ou fallback préciser.
        # On NE fait pas de return ici pour laisser une chance à python-magic.

    # ======================
    # 1. python-magic, si dispo
    # ======================
    if HAVE_MAGIC:
        try:
            # Description "humaine"
            desc_magic = magic.from_buffer(data)
        except Exception:
            desc_magic = None

        try:
            mime = magic.from_buffer(data, mime=True)
        except Exception:
            mime = None

        # Si on a un MIME exploitable
        if mime:
            # Cas particulier ZIP : on utilise detect_zip_subtype pour raffiner
            if mime == "application/zip" and path_for_containers is not None:
                return detect_zip_subtype(path_for_containers)

            ext = mime_to_extension(mime)
            if ext is not None:
                return ext, (desc_magic or f"Detected by python-magic: {mime}")

    # ======================
    # 2. Fallback : ancien détecteur par magic numbers
    # ======================
    return detect_file_type_manual(data, path_for_containers=path_for_containers)


def main():
    global TrueExtension

    # Détermination du chemin du fichier à scanner
    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        path = FilePathScan  # utilise le placeholder

    if not path or path == "FilePathScan":
        print("Erreur : veuillez renseigner le chemin dans FilePathScan ou passer un chemin en argument.")
        sys.exit(1)

    if not os.path.isfile(path):
        print(f"Erreur : le fichier n'existe pas : {path}")
        sys.exit(1)

    # Lecture des premiers octets
    data = read_file_start(path)

    # Détection
    ext, desc = detect_file_type(data, path_for_containers=path)
    TrueExtension = ext  # <-- variable demandée

    print(f"Chemin : {path}")
    print(f"TrueExtension : {repr(TrueExtension)}")
    print(f"Description détectée : {desc}")

    # Optionnel : si aucune extension détectée, indiquer qu'on ne sait pas
    if ext is None:
        print("Aucune extension probable n'a pu être déduite (format inconnu ou non pris en charge).")

    if not HAVE_MAGIC:
        print("\nAttention : python-magic n'est pas installé, seul le fallback par magic numbers a été utilisé.")


if __name__ == "__main__":
    main()
