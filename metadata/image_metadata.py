# image_metadata.py
import sqlite3
from pathlib import Path
from typing import Optional, Tuple, Any, Dict

from PIL import Image, ExifTags

# ----------------------------
# --- PREPARING EXTRACTION ---
# ----------------------------

# --- Helpers EXIF & GPS ---
def _rational_to_float(value: Any) -> Optional[float]:
    # """
    # Convertit un rationnel EXIF (type (num, den) ou Fraction) en float.
    # """ 
    try:
        if isinstance(value, (int, float)):
            return float(value)
        # cas tuple (num, den)
        if isinstance(value, tuple) and len(value) == 2:
            num, den = value
            if den:
                return float(num) / float(den)
        # certains EXIF peuvent renvoyer un objet "IFDRational"
        return float(value)
    except Exception:
        return None


def _convert_gps_coord(coord, ref) -> Optional[float]:
    """
    Convertit les coordonnées GPS EXIF (degrés, minutes, secondes)
    en degrés décimaux.
    coord = [(d_num, d_den), (m_num, m_den), (s_num, s_den)]
    ref = 'N', 'S', 'E', 'W'
    """
    if not coord or len(coord) != 3 or not ref:
        return None
    try:
        d = _rational_to_float(coord[0])
        m = _rational_to_float(coord[1])
        s = _rational_to_float(coord[2])
        if d is None or m is None or s is None:
            return None
        decimal = d + m / 60.0 + s / 3600.0
        if ref in ("S", "W"):
            decimal = -decimal
        return decimal
    except Exception:
        return None


def _extract_exif_dict(img: Image.Image) -> Dict[str, Any]:
    """
    Récupère un dict {tag_name: value} à partir de l'EXIF brut de Pillow.
    """
    exif_data = {}
    raw_exif = getattr(img, "_getexif", lambda: None)() or {}
    for tag_id, value in raw_exif.items():
        tag_name = ExifTags.TAGS.get(tag_id, str(tag_id))
        exif_data[tag_name] = value
    return exif_data


def _extract_gps(exif_data: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Extrait latitude, longitude, altitude à partir de exif_data['GPSInfo'].
    """
    gps_info = exif_data.get("GPSInfo")
    if not gps_info:
        return None, None, None

    # Remap GPS Data
    gps_decoded = {}
    for key, val in gps_info.items():
        name = ExifTags.GPSTAGS.get(key, str(key))
        gps_decoded[name] = val

    lat = lon = alt = None

    lat = _convert_gps_coord(
        gps_decoded.get("GPSLatitude"),
        gps_decoded.get("GPSLatitudeRef"),
    )
    lon = _convert_gps_coord(
        gps_decoded.get("GPSLongitude"),
        gps_decoded.get("GPSLongitudeRef"),
    )

    # Altitude
    alt_raw = gps_decoded.get("GPSAltitude")
    if alt_raw is not None:
        alt_val = _rational_to_float(alt_raw)
        if alt_val is not None:
            ref = gps_decoded.get("GPSAltitudeRef", 0)
            # 0 = au-dessus du niveau de la mer, 1 = en-dessous
            if ref == 1:
                alt_val = -alt_val
            alt = alt_val

    return lat, lon, alt


def _convert_exif_datetime_to_iso(dt: str) -> Optional[str]:
    """
    Convertit un datetime EXIF 'YYYY:MM:DD HH:MM:SS'
    en 'YYYY-MM-DDTHH:MM:SS' (ISO8601 simplifié).
    """
    if not dt:
        return None
    try:
        # Format standard EXIF
        date_part, time_part = dt.split(" ")
        y, m, d = date_part.split(":")
        return f"{y}-{m}-{d}T{time_part}"
    except Exception:
        return None

# -------------------------
# --- MAIN DATA EXTRACT ---
# -------------------------


def extract_image_metadata_from_path(path: str) -> dict:
    """
    Ouvre l'image avec Pillow et renvoie un dict contenant
    les métadonnées à insérer / mettre à jour dans file_image_metadata,
    plus quelques infos utiles pour la table 'file' (mime, ...).

    Ne touche pas à la base de données, renvoie juste un dict Python.
    """
    metadata = {
        # Fields for image_metadata
        "image_type": None,
        "format": None,
        "width_px": None,
        "height_px": None,
        "dpi_x": None,
        "dpi_y": None,
        "bits_per_channel": None,
        "bits_per_pixel": None,
        "color_space": None,
        "has_alpha": None,
        "orientation": None,
        "has_embedded_thumbnail": None,
        "exif_datetime_original": None,
        "camera_make": None,
        "camera_model": None,
        "lens_model": None,
        "focal_length_mm": None,
        "aperture_f": None,
        "exposure_time_s": None,
        "iso": None,
        "flash_used": None,
        "gps_lat": None,
        "gps_lon": None,
        "gps_alt": None,
        "author": None,
        "title": None,
        "description": None,
        "keywords": None,
        "copyright": None,
        "software": None,

        # Quelques infos pour la table 'file'
        "mime_detected": None,
    }

    img = Image.open(path)
    img.load()  # force le chargement

    # --- Format / dimensions / DPI ---
    metadata["format"] = (img.format or "").lower() or None
    width, height = img.size
    metadata["width_px"] = width
    metadata["height_px"] = height

    # DPI 
    dpi = img.info.get("dpi") or img.info.get("resolution")
    if isinstance(dpi, tuple) and len(dpi) >= 2:
        metadata["dpi_x"] = float(dpi[0])
        metadata["dpi_y"] = float(dpi[1])

    # Type raster
    metadata["image_type"] = "raster"

    # Bits per canal / pixel
    mode = img.mode  # ex: 'RGB', 'RGBA', 'L', 'CMYK'
    bands = img.getbands()  # ex: ('R', 'G', 'B')
    n_channels = len(bands)

    # Most common appx
    if mode in ("1",):
        bits_per_channel = 1
    elif mode in ("L", "P", "RGB", "RGBA", "CMYK", "YCbCr", "LAB"):
        bits_per_channel = 8
    elif mode.startswith("I;16") or mode == "I;16":
        bits_per_channel = 16
    else:
        bits_per_channel = None

    metadata["bits_per_channel"] = bits_per_channel
    if bits_per_channel is not None:
        metadata["bits_per_pixel"] = bits_per_channel * n_channels

    # Basic colour space
    if mode in ("L", "P", "1"):
        metadata["color_space"] = "Gray"
    elif mode in ("RGB", "RGBA", "P", "YCbCr"):
        metadata["color_space"] = "sRGB"
    elif mode == "CMYK":
        metadata["color_space"] = "CMYK"
    else:
        metadata["color_space"] = None

    # Alpha
    metadata["has_alpha"] = 1 if "A" in bands or mode in ("LA", "RGBA", "PA") else 0

    # MIME
    try:
        from PIL import Image as PILImage
        mime = PILImage.MIME.get(img.format)
        metadata["mime_detected"] = mime
    except Exception:
        metadata["mime_detected"] = None

    # --- EXIF ---
    exif_data = _extract_exif_dict(img)

    # Orientation
    orientation = exif_data.get("Orientation")
    if isinstance(orientation, int):
        metadata["orientation"] = orientation

    # Date & hour
    dt_original = exif_data.get("DateTimeOriginal") or exif_data.get("DateTime")
    metadata["exif_datetime_original"] = _convert_exif_datetime_to_iso(dt_original)

    # Hardware & lens
    metadata["camera_make"] = exif_data.get("Make")
    metadata["camera_model"] = exif_data.get("Model")
    metadata["lens_model"] = exif_data.get("LensModel")

    # Focus
    focal = exif_data.get("FocalLength")
    metadata["focal_length_mm"] = _rational_to_float(focal)

    # Apperture
    fnumber = exif_data.get("FNumber")
    metadata["aperture_f"] = _rational_to_float(fnumber)

    # Exposure
    exposure = exif_data.get("ExposureTime")
    metadata["exposure_time_s"] = _rational_to_float(exposure)

    # ISO
    iso = exif_data.get("ISOSpeedRatings") or exif_data.get("PhotographicSensitivity")
    if isinstance(iso, (list, tuple)):
        iso = iso[0]
    metadata["iso"] = int(iso) if isinstance(iso, (int, float)) else None

    # Flash
    flash = exif_data.get("Flash")
    if flash is not None:
        # 0 => pas de flash, sinon on considère flash_used = 1
        metadata["flash_used"] = 0 if flash == 0 else 1

    # GPS
    gps_lat, gps_lon, gps_alt = _extract_gps(exif_data)
    metadata["gps_lat"] = gps_lat
    metadata["gps_lon"] = gps_lon
    metadata["gps_alt"] = gps_alt

    # Autor and basic tags
    metadata["author"] = exif_data.get("Artist")
    metadata["title"] = exif_data.get("ImageDescription")
    metadata["description"] = exif_data.get("UserComment")
    metadata["copyright"] = exif_data.get("Copyright")
    metadata["software"] = exif_data.get("Software")

    # Keywords / XMP etc. : dépendant du logiciel, souvent dans des tags
    # propriétaires (XPKeywords, etc). Ici, on ne les gère pas.
    metadata["keywords"] = None

    # Vignette embarquée (approximatif)
    # On considère qu'il y a une vignette si GPSInfo ou d'autres blocs EXIF
    # spécifiques existent ; sinon on peut tenter les tags 513/514.
    # Si tu veux être plus précis, tu peux inspecter exif_data en détail.
    meta_keys = exif_data.keys()
    has_thumb = 1 if 513 in exif_data or 514 in exif_data else 0
    metadata["has_embedded_thumbnail"] = has_thumb

    return metadata

# -------------------------
# --- POPULATE DATABASE ---
# -------------------------

def populate_image_metadata(conn: sqlite3.Connection, file_id: int) -> None:
    """
    Lit le chemin du fichier dans la table 'file',
    extrait les métadonnées image, et met à jour :
      - file_image_metadata
      - file.mime_detected
    pour le file_id donné.
    """
    cur = conn.cursor()

    row = cur.execute(
        "SELECT path FROM file WHERE id = ?",
        (file_id,),
    ).fetchone()

    if row is None:
        raise ValueError(f"Aucun fichier avec id={file_id}")

    path = row[0]

    # Extraction via Pillow
    meta = extract_image_metadata_from_path(path)

    # Mise à jour de file_image_metadata
    cur.execute(
        """
        UPDATE file_image_metadata
           SET image_type             = ?,
               format                 = ?,
               width_px               = ?,
               height_px              = ?,
               dpi_x                  = ?,
               dpi_y                  = ?,
               bits_per_channel       = ?,
               bits_per_pixel         = ?,
               color_space            = ?,
               has_alpha              = ?,
               orientation            = ?,
               has_embedded_thumbnail = ?,
               exif_datetime_original = ?,
               camera_make            = ?,
               camera_model           = ?,
               lens_model             = ?,
               focal_length_mm        = ?,
               aperture_f             = ?,
               exposure_time_s        = ?,
               iso                    = ?,
               flash_used             = ?,
               gps_lat                = ?,
               gps_lon                = ?,
               gps_alt                = ?,
               author                 = ?,
               title                  = ?,
               description            = ?,
               keywords               = ?,
               copyright              = ?,
               software               = ?
         WHERE file_id = ?
        """,
        (
            meta["image_type"],
            meta["format"],
            meta["width_px"],
            meta["height_px"],
            meta["dpi_x"],
            meta["dpi_y"],
            meta["bits_per_channel"],
            meta["bits_per_pixel"],
            meta["color_space"],
            meta["has_alpha"],
            meta["orientation"],
            meta["has_embedded_thumbnail"],
            meta["exif_datetime_original"],
            meta["camera_make"],
            meta["camera_model"],
            meta["lens_model"],
            meta["focal_length_mm"],
            meta["aperture_f"],
            meta["exposure_time_s"],
            meta["iso"],
            meta["flash_used"],
            meta["gps_lat"],
            meta["gps_lon"],
            meta["gps_alt"],
            meta["author"],
            meta["title"],
            meta["description"],
            meta["keywords"],
            meta["copyright"],
            meta["software"],
            file_id,
        ),
    )

    # Mise à jour de file (mime_detected + updated_at)
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
