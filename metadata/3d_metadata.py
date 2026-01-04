# metadata/3d_metadata.py
import sqlite3
import os
import struct
import json
from typing import Dict, Any, Tuple

# ----------------------------
# --- HELPERS ---
# ----------------------------

def _parse_stl(path: str) -> Dict[str, Any]:
    """Analyse un fichier STL (Stereolithography)."""
    stats = {"vertex_count": 0, "face_count": 0, "is_binary": 0, "info": ""}
    
    try:
        # Détection ASCII vs Binaire
        # STL ASCII commence par "solid", STL binaire a un header de 80 bytes
        is_binary = False
        with open(path, 'rb') as f:
            header = f.read(80)
            if b'solid' in header[:5]:
                # Faux positif possible, mais généralement fiable. 
                # On vérifie si c'est vraiment du texte
                try:
                    with open(path, 'r', encoding='ascii') as t:
                        t.read(1024)
                        is_binary = False
                except:
                    is_binary = True
            else:
                is_binary = True
        
        stats["is_binary"] = 1 if is_binary else 0
        
        if is_binary:
            # STL Binaire: 80 bytes header + 4 bytes (uint32) triangle count
            file_size = os.path.getsize(path)
            if file_size >= 84:
                with open(path, 'rb') as f:
                    f.seek(80)
                    count_bytes = f.read(4)
                    face_count = struct.unpack('<I', count_bytes)[0]
                    stats["face_count"] = face_count
                    # En STL, les sommets ne sont pas partagés/indexés dans le format brut,
                    # donc 3 sommets par face.
                    stats["vertex_count"] = face_count * 3
                    stats["info"] = f"Binary STL Header: {header.decode('ascii', errors='replace').strip()}"
        else:
            # STL ASCII: on compte les lignes 'facet' et 'vertex'
            v_count = 0
            f_count = 0
            with open(path, 'r', encoding='ascii', errors='ignore') as f:
                for line in f:
                    s = line.strip()
                    if s.startswith('vertex'):
                        v_count += 1
                    elif s.startswith('facet'):
                        f_count += 1
            stats["vertex_count"] = v_count
            stats["face_count"] = f_count
            stats["info"] = "ASCII STL"
            
    except Exception as e:
        stats["info"] = f"Error parsing STL: {e}"
        
    return stats

def _parse_obj(path: str) -> Dict[str, Any]:
    """Analyse un fichier Wavefront OBJ (Texte)."""
    stats = {"vertex_count": 0, "face_count": 0, "is_binary": 0, "info": "Wavefront OBJ"}
    
    try:
        v_count = 0
        f_count = 0
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if line.startswith('v '):
                    v_count += 1
                elif line.startswith('f '):
                    f_count += 1
        
        stats["vertex_count"] = v_count
        stats["face_count"] = f_count
        
    except Exception as e:
        stats["info"] = f"Error parsing OBJ: {e}"
        
    return stats

def _parse_ply(path: str) -> Dict[str, Any]:
    """Analyse un fichier PLY (Stanford). Lit le header."""
    stats = {"vertex_count": 0, "face_count": 0, "is_binary": 0, "info": ""}
    
    try:
        with open(path, 'rb') as f:
            # Lecture du header ligne par ligne jusqu'à "end_header"
            header_content = b""
            while True:
                line = f.readline()
                header_content += line
                line_str = line.decode('ascii', errors='ignore').strip()
                
                if line_str.startswith("format binary"):
                    stats["is_binary"] = 1
                elif line_str.startswith("element vertex"):
                    # ex: "element vertex 12345"
                    parts = line_str.split()
                    if len(parts) >= 3:
                        stats["vertex_count"] = int(parts[2])
                elif line_str.startswith("element face"):
                    parts = line_str.split()
                    if len(parts) >= 3:
                        stats["face_count"] = int(parts[2])
                
                if line_str == "end_header" or len(header_content) > 5000:
                    break
            
            stats["info"] = f"PLY Header detected. Format: {'Binary' if stats['is_binary'] else 'ASCII'}"

    except Exception as e:
        stats["info"] = f"Error parsing PLY: {e}"
        
    return stats

def _parse_gltf_glb(path: str, ext: str) -> Dict[str, Any]:
    """Analyse basique GLTF (JSON) ou GLB (Binary)."""
    stats = {"vertex_count": 0, "face_count": 0, "is_binary": 0, "info": ""}
    
    try:
        json_data = None
        
        if ext == '.gltf':
            stats["is_binary"] = 0
            stats["info"] = "GLTF JSON"
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                json_data = json.load(f)
        
        elif ext == '.glb':
            stats["is_binary"] = 1
            stats["info"] = "GLTF Binary (GLB)"
            with open(path, 'rb') as f:
                # Header 12 bytes: magic(4), version(4), length(4)
                magic = f.read(4)
                if magic == b'glTF':
                    f.read(8) # Skip version & length
                    # Chunk 0 Header: length(4), type(4) -> type must be JSON (0x4E4F534A)
                    chunk_len = struct.unpack('<I', f.read(4))[0]
                    chunk_type = f.read(4)
                    if chunk_type == b'JSON':
                        json_bytes = f.read(chunk_len)
                        json_data = json.loads(json_bytes)

        if json_data:
            # GLTF ne stocke pas directement le count total simple,
            # il faut regarder les accessors. C'est complexe sans parser tout.
            # On compte simplement le nombre de meshes/primitives déclarés comme proxy.
            meshes = json_data.get('meshes', [])
            stats["info"] += f" | Meshes: {len(meshes)}"
            
            # Estimation très grossière via les accessors 'count' si possible
            # (souvent l'accessor 0 est les positions)
            accessors = json_data.get('accessors', [])
            total_count = 0
            for acc in accessors:
                # Si type VEC3 (souvent positions)
                if acc.get('type') == 'VEC3':
                    total_count += acc.get('count', 0)
            
            # Ce n'est pas précis car VEC3 peut être normales, couleurs, etc.
            # On laisse à 0 si incertain ou on met le max trouvé comme proxy
            if total_count > 0:
                stats["vertex_count"] = total_count # Proxy

    except Exception as e:
        stats["info"] += f" Error: {e}"

    return stats

# -------------------------
# --- MAIN DATA EXTRACT ---
# -------------------------

def extract_3d_metadata_from_path(path: str) -> Dict[str, Any]:
    """
    Extrait les métadonnées d'un fichier 3D.
    """
    meta = {
        "format_type": "Unknown",
        "vertex_count": 0,
        "face_count": 0,
        "mesh_count": 1, # Défaut
        "is_binary": 0,
        "has_textures": 0,
        "mime_detected": "application/octet-stream",
        "Exerpt_hund": None,
        "Exerpt_thou": None,
        "Exerpt_full": None
    }

    if not os.path.exists(path):
        return meta

    ext = os.path.splitext(path)[1].lower()
    stats = {}

    # 1. Dispatch par format
    if ext == '.stl':
        meta["format_type"] = "STL"
        meta["mime_detected"] = "model/stl"
        stats = _parse_stl(path)
        
    elif ext == '.obj':
        meta["format_type"] = "OBJ"
        meta["mime_detected"] = "model/obj"
        stats = _parse_obj(path)
        # OBJ a souvent des textures référencées (.mtl)
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                if 'mtllib' in f.read(2048):
                    meta["has_textures"] = 1
        except: pass

    elif ext == '.ply':
        meta["format_type"] = "PLY"
        meta["mime_detected"] = "model/ply"
        stats = _parse_ply(path)

    elif ext in ['.gltf', '.glb']:
        meta["format_type"] = "GLTF"
        meta["mime_detected"] = "model/gltf+json" if ext == '.gltf' else "model/gltf-binary"
        stats = _parse_gltf_glb(path, ext)
        meta["has_textures"] = 1 # GLTF supporte quasi toujours les textures

    elif ext == '.fbx':
        meta["format_type"] = "FBX"
        meta["mime_detected"] = "application/vnd.autodesk.fbx"
        # FBX est très complexe (format propriétaire souvent binaire).
        # On met juste un placeholder binaire.
        stats = {"vertex_count": 0, "face_count": 0, "is_binary": 1, "info": "FBX (Complex format)"}
        with open(path, 'rb') as f:
            if f.read(18) == b'Kaydara FBX Binary':
                stats["is_binary"] = 1
            else:
                stats["is_binary"] = 0
                stats["info"] = "FBX ASCII"

    else:
        # Fallback générique
        pass

    # 2. Mapping résultats
    if stats:
        meta["vertex_count"] = stats.get("vertex_count", 0)
        meta["face_count"] = stats.get("face_count", 0)
        meta["is_binary"] = stats.get("is_binary", 0)
        
        info = stats.get("info", "")
        
        # 3. Construction Exerpts
        summary_lines = [
            f"FORMAT: {meta['format_type']}",
            f"ENCODING: {'Binary' if meta['is_binary'] else 'ASCII'}",
            f"VERTICES: {meta['vertex_count']}",
            f"FACES: {meta['face_count']}",
            f"INFO: {info}"
        ]
        
        full_text = "\n".join(summary_lines)
        
        meta["Exerpt_full"] = full_text
        meta["Exerpt_thou"] = full_text[:1000]
        meta["Exerpt_hund"] = full_text[:100]

    return meta


# -------------------------
# --- POPULATE DATABASE ---
# -------------------------

def populate_3d_metadata(conn: sqlite3.Connection, file_id: int) -> None:
    """
    Lit le chemin du fichier dans la table 'file',
    extrait les métadonnées 3D, et insère/met à jour :
      - file_3d_metadata
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
    meta = extract_3d_metadata_from_path(path)
    
    # Insertion
    cur.execute(
        """
        INSERT OR REPLACE INTO file_3d_metadata (
            file_id,
            format_type,
            vertex_count,
            face_count,
            mesh_count,
            is_binary,
            has_textures,
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
            meta["vertex_count"],
            meta["face_count"],
            meta["mesh_count"],
            meta["is_binary"],
            meta["has_textures"],
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