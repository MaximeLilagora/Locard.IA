import re
import sqlite3
from pathlib import Path
from typing import List, Dict, Tuple

# -------------------------------
# 🔍 DÉFINITION DES REGEX (16 CATÉGORIES)
# -------------------------------

REGEXES = {
    "donnees_personnelles": {
        "pattern": re.compile(r"(Nom|Prénom|Date de naissance|Adresse|Ville|Département|Code postal|N° de dossier)\s*[:=]?\s*[\w\s\-\./,]{5,}", re.IGNORECASE),
        "description": "Données personnelles (RGPD)"
    },
    "nss": {
        "pattern": re.compile(r"\b\d{3}\s\d{3}\s\d{3}\s\d{4}\b"),
        "description": "Numéro de Sécurité Sociale (NSS)"
    },
    "carte_identite_passeport": {
        "pattern": re.compile(r"\b\d{12}|\b[A-Z]\d{7}\b"),
        "description": "Carte d’identité / Passeport"
    },
    "telephone": {
        "pattern": re.compile(r"\b(?:0[1-9]|\+33[1-9])\s?\d{2}\s?\d{2}\s?\d{2}\s?\d{2}\b"),
        "description": "Téléphone français"
    },
    "email": {
        "pattern": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
        "description": "Email personnel/professionnel"
    },
    "mot_de_passe_cle_api": {
        "pattern": re.compile(r"(pass|password|api[_-]?key|secret|token|access[_-]?key)\s*=\s*[\w\.\+\-]{8,}", re.IGNORECASE),
        "description": "Mot de passe ou clé API"
    },
    "carte_bancaire": {
        "pattern": re.compile(r"\b(?:\d{4}[-\s]?){3}\d{4}\b"),
        "description": "Carte bancaire (CB)"
    },
    "iban_bic": {
        "pattern": re.compile(r"\bFR\d{2}\s?\d{4}\s?\d{4}\s?\d{4}\s?\d{4}\s?\d{4}\b|\b[A-Z]{4}\d{2}[A-Z0-9]{14}\b"),
        "description": "IBAN ou BIC"
    },
    "fichiers_temporaires": {
        "pattern": re.compile(r"\.tmp|\.backup|\.old|\.DS_Store|\.env\.bak|\.~|\.gitignore|\.gitmodules", re.IGNORECASE),
        "description": "Fichiers temporaires/cachés"
    },
    "url_internes": {
        "pattern": re.compile(r"(localhost|127\.0\.0\.1|192\.168\.|10\.|internal|dev|staging|test)\:\d+", re.IGNORECASE),
        "description": "URL internes ou non sécurisées"
    },
    "localisation": {
        "pattern": re.compile(r"(Lat|Latitude|Long|Longitude|Coordonnées|Adresse|Ville|Département|Code postal)\s*[:=]?\s*[\d\.\-\+]+", re.IGNORECASE),
        "description": "Coordonnées géographiques"
    },
    "identifiants_machines": {
        "pattern": re.compile(r"(MAC|UUID|Serial|Hostname|Machine|Computer)\s*[:=]?\s*([0-9a-fA-F:\-\.\s]{12,})", re.IGNORECASE),
        "description": "Identifiants de machines"
    },
    "utilisateurs_roles": {
        "pattern": re.compile(r"(User|Utilisateur|Login|Username|Fonction|Role|Rôle)\s*[:=]?\s*([A-Za-z\s\-]{3,})", re.IGNORECASE),
        "description": "Utilisateurs ou rôles"
    },
    "historique_fichiers": {
        "pattern": re.compile(r"(modified|deleted|chmod|chown|mv|cp|rm|rename)\s+[/\w\.\-\s]+", re.IGNORECASE),
        "description": "Historique de modification de fichiers"
    },
    "extensions_suspectes": {
        "pattern": re.compile(r"\.(exe|bat|sh|cmd|py|js|php|pl|ini|env|bak|backup|old|tmp|log|sql|json|xml|yml|yaml|conf|cfg)$", re.IGNORECASE),
        "description": "Extensions de fichiers suspectes"
    },
    "commentaires_sensibles": {
        "pattern": re.compile(r"(À\s+supprimer|WARNING|SENSIBLE|SECRET|CONFIDENTIEL|TODO|TEST|danger|important|ne\s+pas\s+publier|à\s+supprimer|ne\s+pas\s+publier)", re.IGNORECASE),
        "description": "Commentaires ou balises sensibles"
    }
}

# -------------------------------
# 📥 FONCTION : Récupérer le texte d’un fichier
# -------------------------------
def get_file_text(conn: sqlite3.Connection, file_id: int) -> str:
    """Récupère le texte (Exerpt_full) d’un fichier depuis la base."""
    try:
        # Essaye d’abord dans file_text_metadata
        cur = conn.execute("""
            SELECT Exerpt_full FROM file_text_metadata WHERE file_id = ?
        """, (file_id,))
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
        
        # Sinon, on peut essayer d’extraire depuis un autre type (ex: office, pdf)
        # Ici, on retourne une chaîne vide si pas trouvé
        return ""
    except Exception as e:
        print(f"⚠️ Erreur lecture texte (file_id={file_id}): {e}")
        return ""

# -------------------------------
# 🧠 FONCTION : Scanner un texte avec toutes les regex
# -------------------------------
def scan_text_with_regex(text: str, file_id: int, conn: sqlite3.Connection) -> List[Dict]:
    """Scanne un texte avec toutes les regex, retourne les résultats."""
    results = []

    for category_name, config in REGEXES.items():
        matches = config["pattern"].findall(text)
        if not matches:
            continue

        for match in matches:
            # Gérer les groupes (ex : mot de passe = mot + valeur)
            if isinstance(match, tuple):
                value = match[1] if len(match) > 1 else match[0]
            else:
                value = match

            value = value.strip()

            # Ajouter dans la liste
            results.append({
                "file_id": file_id,
                "category": config["description"],
                "value": value,
                "line_number": None,  # Optionnel : tu peux parser par ligne si besoin
                "char_offset": None,
            })

    return results

# -------------------------------
# 💾 FONCTION : Enregistrer les résultats dans la base
# -------------------------------
def save_detections_to_db(results: List[Dict], conn: sqlite3.Connection):
    """Enregistre les détections dans la table file_sensitivity_detection."""
    if not results:
        return

    try:
        conn.executemany("""
            INSERT OR IGNORE INTO file_sensitivity_detection 
            (file_id, category, value, detected_at)
            VALUES (?, ?, ?, datetime('now'))
        """, [
            (r["file_id"], r["category"], r["value"])
            for r in results
        ])
        conn.commit()
        print(f"✅ {len(results)} détections sauvegardées.")
    except Exception as e:
        print(f"❌ Erreur sauvegarde : {e}")
        conn.rollback()

# -------------------------------
# 🚀 FONCTION PRINCIPALE : Scanner tous les fichiers
# -------------------------------
def run_forensic_scan(db_path: str = "working_DB/project_index.db"):
    """Scanne tous les fichiers de la base avec les regex."""
    print("🔍 Début du scan forensic (16 catégories)...")
    
    conn = sqlite3.connect(db_path)
    try:
        # Récupérer tous les fichiers qui ont du texte
        cur = conn.execute("""
            SELECT f.id, f.path, t.Exerpt_full 
            FROM file f 
            JOIN file_text_metadata t ON f.id = t.file_id
            WHERE t.Exerpt_full IS NOT NULL AND t.Exerpt_full != ''
        """)
        
        total_files = 0
        total_detections = 0

        for row in cur.fetchall():
            file_id = row[0]
            path = row[1]
            text = row[2]

            print(f"📄 Analyse : {path} (ID: {file_id})")
            detections = scan_text_with_regex(text, file_id, conn)
            save_detections_to_db(detections, conn)
            
            total_files += 1
            total_detections += len(detections)

        print(f"\n✅ Analyse terminée : {total_files} fichiers analysés, {total_detections} détections trouvées.")

    except Exception as e:
        print(f"❌ Erreur générale : {e}")
    finally:
        conn.close()

# -------------------------------
# 🧪 TEST : Exécuter le scan
# -------------------------------
if __name__ == "__main__":
    run_forensic_scan()
