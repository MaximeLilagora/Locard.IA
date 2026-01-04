# metadata/video_metadata.py
import sqlite3
import subprocess
import json
import shutil
import os
from pathlib import Path
from typing import Optional, Dict, Any

# Vérification de la présence de ffprobe au chargement du module
FFPROBE_BIN = shutil.which("ffprobe")

# ----------------------------
# --- HELPERS ---
# ----------------------------

def _parse_frame_rate(r_frame_rate: str) -> Optional[float]:
    """
    Convertit le format '30000/1001' ou '30/1' de ffprobe en float.
    """
    if not r_frame_rate or r_frame_rate == "0/0":
        return None
    try:
        if "/" in r_frame_rate:
            num, den = r_frame_rate.split("/")
            if float(den) != 0:
                return float(num) / float(den)
        else:
            return float(r_frame_rate)
    except ValueError:
        pass
    return None

def _get_codec_name(stream: dict) -> str:
    """Récupère le nom lisible du codec."""
    # Parfois 'codec_name' est technique (h264), 'codec_long_name' est verbeux
    # On préfère codec_name en majuscule, ou profile si dispo
    name = stream.get("codec_name", "unknown")
    profile = stream.get("profile")
    
    if name == "h264" and profile:
        return f"H.264 ({profile})"
    if name == "hevc":
        return "H.265/HEVC"
    return name.upper()

# -------------------------
# --- MAIN DATA EXTRACT ---
# -------------------------

def extract_video_metadata_from_path(path: str) -> Dict[str, Any]:
    """
    Extrait les métadonnées vidéo via ffprobe (JSON output).
    Retourne un dictionnaire prêt pour l'insertion SQL.
    """
    metadata = {
        "width": None,
        "height": None,
        "duration_sec": None,
        "video_codec": None,
        "audio_codec": None,
        "container_format": None,
        "frame_rate": None,
        "bitrate_kbps": None,
        "mime_detected": None # Pour mise à jour table file
    }

    if not FFPROBE_BIN:
        print(f"ATTENTION: 'ffprobe' introuvable. Impossible de scanner la vidéo : {path}")
        return metadata

    try:
        # Commande ffprobe pour sortir un JSON propre
        # -v error : mode silencieux
        # -show_format : conteneur, durée, bitrate global
        # -show_streams : détails vidéo/audio
        cmd = [
            FFPROBE_BIN,
            "-v", "error",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
            path
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        
        if result.returncode != 0:
            print(f"Erreur ffprobe sur {path}: {result.stderr}")
            return metadata

        data = json.loads(result.stdout)
        
        # 1. Container info (Format)
        fmt = data.get("format", {})
        metadata["container_format"] = fmt.get("format_long_name", fmt.get("format_name"))
        
        dur = fmt.get("duration")
        if dur:
            metadata["duration_sec"] = float(dur)
            
        br = fmt.get("bit_rate")
        if br:
            metadata["bitrate_kbps"] = int(br) / 1000.0

        # Mime fallback via format name si possible
        fmt_name = fmt.get("format_name", "").lower()
        if "mp4" in fmt_name: metadata["mime_detected"] = "video/mp4"
        elif "matroska" in fmt_name: metadata["mime_detected"] = "video/x-matroska"
        elif "avi" in fmt_name: metadata["mime_detected"] = "video/x-msvideo"
        elif "webm" in fmt_name: metadata["mime_detected"] = "video/webm"

        # 2. Streams info
        streams = data.get("streams", [])
        
        video_stream = next((s for s in streams if s.get("codec_type") == "video"), None)
        audio_stream = next((s for s in streams if s.get("codec_type") == "audio"), None)

        if video_stream:
            metadata["width"] = video_stream.get("width")
            metadata["height"] = video_stream.get("height")
            metadata["video_codec"] = _get_codec_name(video_stream)
            
            # Framerate
            fps = video_stream.get("r_frame_rate") # "30/1"
            # Fallback avg_frame_rate si r_frame_rate est bizarre
            if fps == "0/0": fps = video_stream.get("avg_frame_rate")
            
            metadata["frame_rate"] = _parse_frame_rate(fps)

        if audio_stream:
            metadata["audio_codec"] = _get_codec_name(audio_stream)

    except Exception as e:
        print(f"Exception lors du scan vidéo {path}: {e}")

    return metadata


# -------------------------
# --- POPULATE DATABASE ---
# -------------------------

def populate_video_metadata(conn: sqlite3.Connection, file_id: int) -> None:
    """
    Lit le chemin du fichier dans la table 'file',
    extrait les métadonnées vidéo via ffprobe, et insère/met à jour :
      - file_video_metadata
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

    # Extraction
    meta = extract_video_metadata_from_path(path)
    
    # Si on n'a rien trouvé (pas de video stream ou erreur), on évite d'insérer du vide
    # sauf si on veut expliciter que c'est vide. Ici on insère si on a au moins un container ou codec.
    if not (meta["container_format"] or meta["video_codec"]):
        return

    # Insertion ou Remplacement
    cur.execute(
        """
        INSERT OR REPLACE INTO file_video_metadata (
            file_id,
            width,
            height,
            duration_sec,
            video_codec,
            audio_codec,
            container_format,
            frame_rate,
            bitrate_kbps
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            file_id,
            meta["width"],
            meta["height"],
            meta["duration_sec"],
            meta["video_codec"],
            meta["audio_codec"],
            meta["container_format"],
            meta["frame_rate"],
            meta["bitrate_kbps"]
        ),
    )

    # Mise à jour du MIME si détecté et plus précis que l'existant
    if meta["mime_detected"]:
        cur.execute(
            """
            UPDATE file
            SET mime_detected = ?,
                updated_at    = datetime('now')
            WHERE id = ? AND (mime_detected IS NULL OR mime_detected = 'application/octet-stream')
            """,
            (meta["mime_detected"], file_id),
        )

    conn.commit()