import sqlite3
import math
import matplotlib.pyplot as plt
import pandas as pd
from collections import Counter
from pathlib import Path

def get_first_two_digits(number):
    """
    Extrait les deux premiers chiffres significatifs d'un nombre (10-99).
    """
    try:
        val = float(number)
        if val <= 0:
            return None
        
        # On déplace la virgule pour avoir un nombre >= 10
        while val < 10:
            val *= 10
        
        # Ou on réduit si trop grand (bien que string conversion soit plus simple souvent)
        # Méthode string pour la robustesse sur les entiers
        s = str(int(number))
        if len(s) >= 2:
            return int(s[:2])
        elif len(s) == 1:
            # Pour un chiffre unique (ex: 5 bytes), on l'ignore souvent en Benford 2-digits
            # ou on considère 50? La norme est d'ignorer < 10 pour le test 2-digits.
            return None
    except Exception:
        return None
    return None

def benford_law_two_digits():
    """
    Génère la distribution théorique de Benford pour les nombres 10 à 99.
    P(d) = log10(1 + 1/d)
    """
    theoretical = {}
    for d in range(10, 100):
        theoretical[d] = math.log10(1 + 1/d)
    return theoretical

def analyze_benford_distribution(db_path: str):
    """
    Exécute l'analyse complète (Extraction -> Calcul -> Graphique).
    Retourne un dictionnaire contenant les stats, l'interprétation et la figure Matplotlib.
    """
    results = {
        "success": False,
        "error": None,
        "chi_square": 0.0,
        "interpretation": "",
        "file_count": 0,
        "fig": None,
        "dataframe": None
    }

    if not Path(db_path).exists():
        results["error"] = f"Base de données introuvable : {db_path}"
        return results

    try:
        # 1. Extraction des données
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # On cible la table 'file' et la colonne 'size_bytes'
        cursor.execute("SELECT size_bytes FROM file WHERE size_bytes IS NOT NULL AND size_bytes > 0")
        rows = cursor.fetchall()
        conn.close()

        sizes = [r[0] for r in rows]
        total_files = len(sizes)
        results["file_count"] = total_files

        if total_files < 50:
            results["error"] = "Pas assez de données pour une analyse fiable (< 50 fichiers)."
            return results

        # 2. Traitement (Extraction des 2 premiers chiffres)
        digits_list = []
        for s in sizes:
            d = get_first_two_digits(s)
            if d is not None:
                digits_list.append(d)
        
        valid_count = len(digits_list)
        if valid_count == 0:
            results["error"] = "Aucune taille valide extraite (fichiers trop petits ?)."
            return results

        # 3. Calcul des fréquences observées
        counter = Counter(digits_list)
        observed_probs = {d: counter.get(d, 0) / valid_count for d in range(10, 100)}

        # 4. Calcul des fréquences théoriques
        theoretical_probs = benford_law_two_digits()

        # 5. Calcul Chi-Carré
        chi_square = 0
        data_rows = []
        
        # Préparation des données pour le graph
        x_values = list(range(10, 100))
        y_obs = [observed_probs[d] * 100 for d in x_values] # en %
        y_theo = [theoretical_probs[d] * 100 for d in x_values] # en %

        for d in range(10, 100):
            obs = observed_probs[d]
            theo = theoretical_probs[d]
            
            # Chi2 contribution: (Obs - Theo)^2 / Theo
            # On travaille souvent sur les comptes réels pour le Chi2 formel, 
            # mais sur les probas pour la comparaison visuelle.
            # Ici adaptation standard sur fréquences normalisées * N
            
            obs_count = obs * valid_count
            theo_count = theo * valid_count
            
            if theo_count > 0:
                chi_square += ((obs_count - theo_count) ** 2) / theo_count
            
            data_rows.append({
                "Digit": d,
                "Observed %": round(obs * 100, 2),
                "Benford %": round(theo * 100, 2),
                "Diff %": round((obs - theo) * 100, 2)
            })

        results["chi_square"] = chi_square
        results["dataframe"] = pd.DataFrame(data_rows)

        # 6. Interprétation
        # Valeur critique Chi2 pour 89 degrés de liberté (90-1) à p=0.05 est env. 112
        if chi_square < 105:
            results["interpretation"] = "✅ Conforme à la loi de Benford (Distribution naturelle)."
        elif chi_square < 130:
            results["interpretation"] = "⚠️ Déviation modérée (Possible anomalie ou échantillon spécifique)."
        else:
            results["interpretation"] = "❌ Déviation significative (Forte probabilité de manipulation ou fichiers générés artificiellement)."

        # 7. Génération du Graphique Matplotlib
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Barres pour l'observé
        ax.bar(x_values, y_obs, width=0.6, label='Observé', color='#4c72b0', alpha=0.8)
        
        # Ligne pour le théorique
        ax.plot(x_values, y_theo, color='#c44e52', linewidth=2, label='Benford (Théorique)')
        
        ax.set_title(f"Analyse Benford (2 digits) - Chi²: {chi_square:.2f}")
        ax.set_xlabel("Deux premiers chiffres (10-99)")
        ax.set_ylabel("Fréquence (%)")
        ax.legend()
        ax.grid(axis='y', linestyle='--', alpha=0.5)
        
        # Optimisation layout
        plt.tight_layout()
        
        results["fig"] = fig
        results["success"] = True

    except Exception as e:
        results["error"] = f"Erreur interne : {str(e)}"

    return results