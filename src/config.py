from pathlib import Path

# Le fichier config.py se trouve dans src/, on remonte d'un niveau pour atteindre la racine du projet
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Chemins utiles
DB_PATH = PROJECT_ROOT / "working_DB" / "project_index.db"