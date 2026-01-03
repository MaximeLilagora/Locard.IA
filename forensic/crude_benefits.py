# forensic/crude_benefits.py

import sqlite3
from pathlib import Path
import io
import csv
from typing import Dict, Any, List, Tuple


def human_readable_size(num_bytes: int) -> str:
    """Retourne une taille lisible (Ko, Mo, Go...)."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num_bytes < 1024:
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.2f} PB"


def analyze_duplicates(db_path: str) -> Dict[str, Any]:
    """
    Analyse les doublons à partir de la colonne hash_sha256 dans la table `file`.

    Retourne un dict avec :
      - groups_count : nombre de groupes de doublons (hash avec ≥ 2 fichiers)
      - removable_files_count : nombre de fichiers qui seraient supprimés
      - wasted_bytes : taille totale économisable
      - csv_bytes : contenu CSV (bytes) listant tous les fichiers dupliqués
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Base SQLite introuvable : {db_path}")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 1) Récupérer tous les fichiers avec un hash non vide
    cur.execute(
        """
        SELECT hash_sha256, path, size_bytes
        FROM file
        WHERE hash_sha256 IS NOT NULL
          AND hash_sha256 <> ''
        ORDER BY hash_sha256, path
        """
    )
    rows: List[Tuple[str, str, int]] = cur.fetchall()
    conn.close()

    # 2) Regrouper par hash
    groups = {}
    for h, path, size in rows:
        if h not in groups:
            groups[h] = []
        groups[h].append((path, size or 0))

    # 3) Ne garder que les groupes avec ≥ 2 fichiers
    duplicate_groups = {h: files for h, files in groups.items() if len(files) > 1}

    groups_count = len(duplicate_groups)
    removable_files_count = 0
    wasted_bytes = 0

    # 4) Construire le CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "hash_sha256",
        "path",
        "size_bytes",
        "group_size",
        "is_kept_reference"  # 1 = fichier "de référence", 0 = doublon à supprimer
    ])

    for h, files in duplicate_groups.items():
        group_size = len(files)

        # On décide de "garder" le premier fichier comme référence
        # et de considérer les autres comme doublons potentiels.
        # Calcul de la taille économisable : somme - max (au cas où les tailles varient)
        sizes = [s for _, s in files]
        total_group_size = sum(sizes)
        max_size = max(sizes) if sizes else 0
        wasted_bytes += total_group_size - max_size
        removable_files_count += group_size - 1

        for idx, (path, size) in enumerate(files):
            is_kept = 1 if idx == 0 else 0
            writer.writerow([h, path, size, group_size, is_kept])

    csv_content = output.getvalue().encode("utf-8")

    return {
        "groups_count": groups_count,
        "removable_files_count": removable_files_count,
        "wasted_bytes": wasted_bytes,
        "wasted_human": human_readable_size(wasted_bytes),
        "csv_bytes": csv_content,
    }