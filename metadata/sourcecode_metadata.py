# metadata/sourcecode_metadata.py
import sqlite3
import os
import re
from typing import Dict, Any, Tuple, Optional

# ----------------------------
# --- LANGUAGE DEFINITIONS ---
# ----------------------------

# Mapping Extension -> Langage
EXT_TO_LANG = {
    '.py': 'Python',
    '.js': 'JavaScript',
    '.ts': 'TypeScript',
    '.html': 'HTML',
    '.css': 'CSS',
    '.java': 'Java',
    '.c': 'C',
    '.cpp': 'C++',
    '.h': 'C/C++ Header',
    '.hpp': 'C++ Header',
    '.cs': 'C#',
    '.php': 'PHP',
    '.rb': 'Ruby',
    '.go': 'Go',
    '.rs': 'Rust',
    '.sql': 'SQL',
    '.sh': 'Shell',
    '.bat': 'Batch',
    '.json': 'JSON',
    '.xml': 'XML',
    '.yaml': 'YAML',
    '.yml': 'YAML',
    '.md': 'Markdown'
}

# Définitions basiques des commentaires pour l'analyse
# (Langage: (SingleLine, MultiLineStart, MultiLineEnd))
COMMENT_SYNTAX = {
    'Python': ('#', '"""', '"""'), # Simplifié, gère aussi '''
    'JavaScript': ('//', '/*', '*/'),
    'TypeScript': ('//', '/*', '*/'),
    'Java': ('//', '/*', '*/'),
    'C': ('//', '/*', '*/'),
    'C++': ('//', '/*', '*/'),
    'C#': ('//', '/*', '*/'),
    'PHP': ('//', '/*', '*/'), # Gère aussi #
    'Ruby': ('#', '=begin', '=end'),
    'Go': ('//', '/*', '*/'),
    'Rust': ('//', '/*', '*/'),
    'SQL': ('--', '/*', '*/'),
    'Shell': ('#', None, None),
    'YAML': ('#', None, None)
}

# ----------------------------
# --- HELPERS ---
# ----------------------------

def _detect_language(path: str) -> str:
    """Détermine le langage basé sur l'extension."""
    ext = os.path.splitext(path)[1].lower()
    return EXT_TO_LANG.get(ext, 'Unknown')

def _count_structures(content: str, language: str) -> Tuple[int, int, int]:
    """
    Compte approximativement les Classes, Fonctions et TODOs via Regex.
    Retourne (class_count, function_count, todo_count).
    """
    classes = 0
    functions = 0
    todos = len(re.findall(r'\b(TODO|FIXME|XXX)\b', content, re.IGNORECASE))

    if language in ['Python']:
        classes = len(re.findall(r'^\s*class\s+\w+', content, re.MULTILINE))
        functions = len(re.findall(r'^\s*def\s+\w+', content, re.MULTILINE))
    
    elif language in ['JavaScript', 'TypeScript', 'Java', 'C++', 'C#', 'PHP', 'Go', 'Rust', 'C']:
        # Regex générique pour "class Name"
        classes = len(re.findall(r'\bclass\s+\w+', content))
        # Regex très approximative pour "functionName(...)" ou "function name(...)"
        # C'est dur à faire parfaitement en regex pure, mais ça donne une idée de la complexité
        functions = len(re.findall(r'\b(function\s+\w+|\w+\s*\(.*\)\s*\{)', content))

    return classes, functions, todos

def _analyze_lines(content: str, language: str) -> Tuple[int, int, int, int]:
    """
    Analyse ligne par ligne.
    Retourne (total, code, comment, blank).
    """
    lines = content.splitlines()
    total = len(lines)
    blank = 0
    comment = 0
    code = 0
    
    syntax = COMMENT_SYNTAX.get(language)
    
    in_multiline = False
    
    for line in lines:
        stripped = line.strip()
        
        if not stripped:
            blank += 1
            continue
            
        if not syntax:
            # Si pas de syntaxe connue, on considère tout comme du code (sauf vide)
            code += 1
            continue
            
        single, multi_start, multi_end = syntax
        
        # Gestion multi-lignes
        if multi_start and multi_end:
            if in_multiline:
                comment += 1
                if multi_end in stripped:
                    in_multiline = False
                continue
            elif multi_start in stripped:
                # Cas simple: le bloc commence ici
                # Attention: il peut commencer et finir sur la même ligne
                if multi_end in stripped and stripped.index(multi_end) > stripped.index(multi_start):
                    comment += 1 # Tout sur la même ligne
                else:
                    comment += 1
                    in_multiline = True
                continue
        
        # Gestion ligne simple
        if single and stripped.startswith(single):
            comment += 1
        else:
            code += 1
            
    return total, code, comment, blank

# -------------------------
# --- MAIN DATA EXTRACT ---
# -------------------------

def extract_code_metadata_from_path(path: str) -> Dict[str, Any]:
    """
    Extrait les métadonnées de code source.
    """
    meta = {
        "language": "Unknown",
        "lines_total": 0,
        "lines_code": 0,
        "lines_comment": 0,
        "lines_empty": 0,
        "class_count": 0,
        "function_count": 0,
        "todo_count": 0,
        "mime_detected": "text/plain",
        "Exerpt_hund": None,
        "Exerpt_thou": None,
        "Exerpt_full": None
    }

    if not os.path.exists(path):
        return meta

    try:
        # Lecture (tentative UTF-8 puis Latin-1)
        content = ""
        try:
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
        except UnicodeDecodeError:
            try:
                with open(path, 'r', encoding='latin-1') as f:
                    content = f.read()
            except:
                return meta # Illisible

        # 1. Détection Langage
        meta["language"] = _detect_language(path)
        
        # Ajustement MIME
        if meta["language"] == 'Python': meta["mime_detected"] = 'text/x-python'
        elif meta["language"] == 'JavaScript': meta["mime_detected"] = 'application/javascript'
        elif meta["language"] == 'HTML': meta["mime_detected"] = 'text/html'
        # ... autres mappings implicites ou on garde text/plain

        # 2. Analyse des lignes (SLOC)
        total, code, cmt, blank = _analyze_lines(content, meta["language"])
        meta["lines_total"] = total
        meta["lines_code"] = code
        meta["lines_comment"] = cmt
        meta["lines_empty"] = blank

        # 3. Analyse Structurelle (Regex)
        cls, fn, todos = _count_structures(content, meta["language"])
        meta["class_count"] = cls
        meta["function_count"] = fn
        meta["todo_count"] = todos

        # 4. Extraits
        meta["Exerpt_full"] = content
        meta["Exerpt_thou"] = content[:1000]
        meta["Exerpt_hund"] = content[:100]

    except Exception as e:
        print(f"Erreur extraction Code {path}: {e}")

    return meta


# -------------------------
# --- POPULATE DATABASE ---
# -------------------------

def populate_code_metadata(conn: sqlite3.Connection, file_id: int) -> None:
    """
    Lit le chemin du fichier dans la table 'file',
    extrait les métadonnées Code, et insère/met à jour :
      - file_code_metadata
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
    meta = extract_code_metadata_from_path(path)
    
    # Insertion
    cur.execute(
        """
        INSERT OR REPLACE INTO file_code_metadata (
            file_id,
            language,
            lines_total,
            lines_code,
            lines_comment,
            lines_empty,
            class_count,
            function_count,
            todo_count,
            Exerpt_hund,
            Exerpt_thou,
            Exerpt_full
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            file_id,
            meta["language"],
            meta["lines_total"],
            meta["lines_code"],
            meta["lines_comment"],
            meta["lines_empty"],
            meta["class_count"],
            meta["function_count"],
            meta["todo_count"],
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