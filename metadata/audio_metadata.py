# metadata/audio_metadata.py
import sqlite3
from pathlib import Path
from typing import Optional, Any, Dict

# Tente d'importer mutagen, gestion gracieuse si absent
try:
    import mutagen
    from mutagen.easyid3 import EasyID3
    from mutagen.mp3 import MP3
    from mutagen.flac import FLAC
    from mutagen.oggvorbis import OggVorbis
    from mutagen.mp4 import MP4
    HAVE_MUTAGEN = True
except ImportError:
    HAVE_MUTAGEN = False

# ----------------------------
# --- HELPERS ---
# ----------------------------

def _get_first(value: Any) -> Optional[Any]:
    """
    Mutagen retourne souvent des listes pour les tags (ex: ['Artist']).
    Cette fonction retourne le premier élément ou la valeur elle-même.
    """
    if isinstance(value, list) or isinstance(value, tuple):
        if len(value) > 0:
            return value[0]
        return None
    return value

def _parse_track_number(track_str: str) -> tuple[Optional[int], Optional[int]]:
    """
    Parse des formats comme "1", "1/12", "01" -> (track_number, track_total)
    """
    if not track_str:
        return None, None
    
    try:
        if "/" in str(track_str):
            parts = str(track_str).split("/")
            num = int(parts[0]) if parts[0].isdigit() else None
            total = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
            return num, total
        elif str(track_str).isdigit():
            return int(track_str), None
    except Exception:
        pass
    return None, None

def _guess_codec_from_mime(mime: str, filename: str) -> str:
    """Devine le codec simplifié pour l'affichage."""
    if not mime:
        return Path(filename).suffix.replace('.', '').upper()
    if "mp3" in mime or "mpeg" in mime: return "MP3"
    if "flac" in mime: return "FLAC"
    if "mp4" in mime or "m4a" in mime: return "AAC/ALAC"
    if "ogg" in mime or "vorbis" in mime: return "Vorbis"
    if "wav" in mime: return "PCM"
    return mime.split("/")[-1].upper()

# -------------------------
# --- MAIN DATA EXTRACT ---
# -------------------------

def extract_audio_metadata_from_path(path: str) -> dict:
    """
    Extrait les métadonnées audio via Mutagen.
    Renvoie un dictionnaire prêt pour l'insertion SQL.
    """
    metadata = {
        # Technical data
        "container_format": None,
        "audio_codec": None,
        "duration_sec": None,
        "bitrate_kbps": None,
        "sample_rate_hz": None,
        "channels": None,
        "is_vbr": 0,
        "loudness_lufs": None, # Nécessite un scan PCM complet (trop lourd ici)
        "peak_db": None,       # Idem
        
        # Tags
        "title": None,
        "artist": None,
        "album": None,
        "album_artist": None,
        "composer": None,
        "genre": None,
        "track_number": None,
        "track_total": None,
        "disc_number": None,
        "disc_total": None,
        "year": None,
        "date": None,
        "has_lyrics": 0,
        "lyrics_language": None,
        "has_cover": 0,
        "label": None,
        "copyright": None,
        "publisher": None,
        "isrc": None,
        "encoder": None,
        
        # Pour la table file
        "mime_detected": None
    }

    if not HAVE_MUTAGEN:
        return metadata

    try:
        audio = mutagen.File(path)
        if not audio:
            return metadata
            
        # --- 1. Technical Info (Stream Info) ---
        if audio.info:
            metadata["duration_sec"] = getattr(audio.info, "length", None)
            
            # Bitrate (souvent en bps, on veut kbps)
            bitrate = getattr(audio.info, "bitrate", None)
            if bitrate:
                metadata["bitrate_kbps"] = bitrate / 1000.0
            
            metadata["sample_rate_hz"] = getattr(audio.info, "sample_rate", None)
            metadata["channels"] = getattr(audio.info, "channels", None)
            
            # VBR detection (disponible sur certains formats comme MP3)
            # Pour FLAC/Vorbis c'est souvent implicite, mais mutagen n'a pas toujours le flag
            # On tente de lire un attribut vbr s'il existe
            is_vbr = getattr(audio.info, "vbr", None) # Parfois c'est bitrate_mode
            if is_vbr is True:
                metadata["is_vbr"] = 1
        
        # --- 2. MIME & Format ---
        # Mutagen donne souvent le MIME. 
        # S'il n'est pas dispo, on le déduit de la classe
        mime = None
        if hasattr(audio, "mime"):
            mime = _get_first(audio.mime)
        
        if not mime:
            # Fallback basique
            if isinstance(audio, MP3): mime = "audio/mpeg"
            elif isinstance(audio, FLAC): mime = "audio/flac"
            elif isinstance(audio, OggVorbis): mime = "audio/ogg"
            elif isinstance(audio, MP4): mime = "audio/mp4"
        
        metadata["mime_detected"] = mime
        metadata["container_format"] = Path(path).suffix.replace('.', '').lower()
        metadata["audio_codec"] = _guess_codec_from_mime(mime, path)

        # --- 3. Tags Extraction ---
        tags = audio.tags
        if tags:
            # Mapping générique : Mutagen utilise des clés différentes selon les formats
            # On essaie de normaliser via EasyID3 pour MP3 ou en cherchant les clés communes
            
            # Titre
            metadata["title"] = _get_first(tags.get("title") or tags.get("TIT2") or tags.get("©nam"))
            
            # Artiste
            metadata["artist"] = _get_first(tags.get("artist") or tags.get("TPE1") or tags.get("©ART"))
            
            # Album
            metadata["album"] = _get_first(tags.get("album") or tags.get("TALB") or tags.get("©alb"))
            
            # Album Artist
            metadata["album_artist"] = _get_first(tags.get("albumartist") or tags.get("TPE2") or tags.get("aART"))
            
            # Composer
            metadata["composer"] = _get_first(tags.get("composer") or tags.get("TCOM") or tags.get("©wrt"))
            
            # Genre
            metadata["genre"] = _get_first(tags.get("genre") or tags.get("TCON") or tags.get("©gen"))
            
            # Date / Year
            date_str = _get_first(tags.get("date") or tags.get("TDRC") or tags.get("TYER") or tags.get("©day"))
            metadata["date"] = str(date_str) if date_str else None
            if date_str:
                # Tentative d'extraire l'année (souvent les 4 premiers chars)
                s_date = str(date_str).strip()
                if len(s_date) >= 4 and s_date[:4].isdigit():
                    metadata["year"] = int(s_date[:4])

            # Pistes (Track)
            track_val = _get_first(tags.get("tracknumber") or tags.get("TRCK") or tags.get("trkn"))
            t_num, t_tot = _parse_track_number(track_val)
            metadata["track_number"] = t_num
            if t_tot: metadata["track_total"] = t_tot
            
            # Disque
            disc_val = _get_first(tags.get("discnumber") or tags.get("TPOS") or tags.get("disk"))
            d_num, d_tot = _parse_track_number(disc_val)
            metadata["disc_number"] = d_num
            if d_tot: metadata["disc_total"] = d_tot

            # Copyright / Label
            metadata["copyright"] = _get_first(tags.get("copyright") or tags.get("TCOP") or tags.get("cprt"))
            metadata["label"] = _get_first(tags.get("organization") or tags.get("TPUB") or tags.get("label"))
            metadata["isrc"] = _get_first(tags.get("isrc") or tags.get("TSRC"))
            metadata["encoder"] = _get_first(tags.get("encoder") or tags.get("TSSE") or tags.get("©too"))
            
            # Lyrics flag (USLT pour ID3, lyrics pour Vorbis)
            if "lyrics" in tags or "USLT" in tags or "©lyr" in tags:
                metadata["has_lyrics"] = 1

            # Cover Art Detection
            # MP3 (APIC), FLAC (Picture Block), MP4 (covr)
            has_cover = False
            if isinstance(audio, MP3):
                # ID3 check for APIC frames
                for key in tags.keys():
                    if key.startswith("APIC:"):
                        has_cover = True
                        break
            elif isinstance(audio, FLAC):
                if getattr(audio, "pictures", None):
                    has_cover = True
            elif isinstance(audio, MP4):
                if "covr" in tags:
                    has_cover = True
            
            if has_cover:
                metadata["has_cover"] = 1

    except Exception as e:
        # En cas d'erreur de parsing (fichier corrompu, format exotique)
        # On retourne ce qu'on a pu extraire (souvent vide)
        print(f"Erreur extraction audio {path}: {e}")
        pass

    return metadata


# -------------------------
# --- POPULATE DATABASE ---
# -------------------------

def populate_audio_metadata(conn: sqlite3.Connection, file_id: int) -> None:
    """
    Lit le chemin du fichier dans la table 'file',
    extrait les métadonnées audio, et insère/met à jour :
      - file_audio_metadata
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

    # Extraction via Mutagen
    meta = extract_audio_metadata_from_path(path)

    # Insertion ou Remplacement dans file_audio_metadata
    # On utilise INSERT OR REPLACE car file_id est la clé primaire
    cur.execute(
        """
        INSERT OR REPLACE INTO file_audio_metadata (
            file_id,
            container_format,
            audio_codec,
            duration_sec,
            bitrate_kbps,
            sample_rate_hz,
            channels,
            is_vbr,
            loudness_lufs,
            peak_db,
            title,
            artist,
            album,
            album_artist,
            composer,
            genre,
            track_number,
            track_total,
            disc_number,
            disc_total,
            year,
            date,
            has_lyrics,
            lyrics_language,
            has_cover,
            label,
            copyright,
            publisher,
            isrc,
            encoder
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            file_id,
            meta["container_format"],
            meta["audio_codec"],
            meta["duration_sec"],
            meta["bitrate_kbps"],
            meta["sample_rate_hz"],
            meta["channels"],
            meta["is_vbr"],
            meta["loudness_lufs"],
            meta["peak_db"],
            meta["title"],
            meta["artist"],
            meta["album"],
            meta["album_artist"],
            meta["composer"],
            meta["genre"],
            meta["track_number"],
            meta["track_total"],
            meta["disc_number"],
            meta["disc_total"],
            meta["year"],
            meta["date"],
            meta["has_lyrics"],
            meta["lyrics_language"],
            meta["has_cover"],
            meta["label"],
            meta["copyright"],
            meta["publisher"],
            meta["isrc"],
            meta["encoder"]
        ),
    )

    # Mise à jour de file (mime_detected si trouvé par Mutagen)
    if meta["mime_detected"]:
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