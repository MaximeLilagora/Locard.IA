import sqlite3
import os
import math
from collections import Counter
import matplotlib.pyplot as plt

def get_file_sizes_from_sqlite(db_path):
    """
    Récupère les tailles de fichiers depuis une base SQLite.
    Adapter selon votre schéma de base de données.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # À ADAPTER selon votre schéma de base de données
    # Exemple : supposons une table 'files' avec une colonne 'size'
    try:
        cursor.execute("SELECT size FROM files WHERE size > 0")
        sizes = [row[0] for row in cursor.fetchall()]
    except sqlite3.OperationalError:
        # Si la table n'existe pas, créer un exemple
        print("Table 'files' non trouvée. Utilisation des tailles d'exemple.")
        sizes = []
    
    conn.close()
    return sizes

def get_first_two_digits(number):
    """
    Extrait les deux premiers chiffres d'un nombre.
    """
    if number == 0:
        return None
    
    # Convertir en chaîne et extraire les chiffres
    number_str = str(int(abs(number)))
    
    if len(number_str) >= 2:
        return int(number_str[:2])
    elif len(number_str) == 1:
        return int(number_str[0])
    return None

def benford_law_two_digits():
    """
    Calcule la distribution théorique de Benford pour les deux premiers chiffres.
    P(d) = log10(1 + 1/d) où d va de 10 à 99
    """
    distribution = {}
    for d in range(10, 100):
        probability = math.log10(1 + 1/d)
        distribution[d] = probability
    return distribution

def calculate_observed_distribution(sizes):
    """
    Calcule la distribution observée des deux premiers chiffres.
    """
    first_two_digits = []
    
    for size in sizes:
        digits = get_first_two_digits(size)
        if digits is not None and digits >= 10:
            first_two_digits.append(digits)
    
    # Compter les occurrences
    counter = Counter(first_two_digits)
    total = len(first_two_digits)
    
    # Convertir en probabilités
    distribution = {}
    for digit in range(10, 100):
        distribution[digit] = counter.get(digit, 0) / total if total > 0 else 0
    
    return distribution, first_two_digits

def compare_distributions(observed, theoretical):
    """
    Compare les distributions observée et théorique.
    """
    print("\n" + "="*70)
    print("COMPARAISON DES DISTRIBUTIONS (2 premiers chiffres)")
    print("="*70)
    print(f"{'Chiffres':<10} {'Observé (%)':<15} {'Benford (%)':<15} {'Différence':<15}")
    print("-"*70)
    
    chi_square = 0
    for digit in range(10, 100):
        obs = observed.get(digit, 0) * 100
        theo = theoretical.get(digit, 0) * 100
        diff = obs - theo
        
        # Calcul du chi-carré
        if theo > 0:
            chi_square += ((obs - theo) ** 2) / theo
        
        # Afficher seulement les chiffres avec occurrence > 0.1%
        if obs > 0.1 or theo > 0.5:
            print(f"{digit:<10} {obs:<15.4f} {theo:<15.4f} {diff:<+15.4f}")
    
    print("-"*70)
    print(f"Chi-carré: {chi_square:.4f}")
    print("="*70)
    
    return chi_square

def plot_distributions(observed, theoretical, output_file='benford_analysis.png'):
    """
    Crée un graphique comparant les deux distributions.
    """
    digits = list(range(10, 100))
    obs_probs = [observed.get(d, 0) * 100 for d in digits]
    theo_probs = [theoretical.get(d, 0) * 100 for d in digits]
    
    plt.figure(figsize=(14, 6))
    
    # Graphique en barres
    width = 0.4
    x = range(len(digits))
    
    plt.bar([i - width/2 for i in x], obs_probs, width, label='Observé', alpha=0.8)
    plt.bar([i + width/2 for i in x], theo_probs, width, label='Benford (théorique)', alpha=0.8)
    
    plt.xlabel('Deux premiers chiffres')
    plt.ylabel('Probabilité (%)')
    plt.title('Distribution de Benford - Deux premiers chiffres')
    plt.legend()
    plt.xticks(range(0, len(digits), 5), [digits[i] for i in range(0, len(digits), 5)], rotation=45)
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\nGraphique sauvegardé : {output_file}")
    plt.show()

def main():
    """
    Fonction principale.
    """
    # Chemin vers votre base de données SQLite
    db_path = "votre_base.db"  # À MODIFIER
    
    print("="*70)
    print("ANALYSE DE LA LOI DE BENFORD SUR BASE DE DONNÉES SQLITE")
    print("="*70)
    
    # Vérifier si la base existe
    if not os.path.exists(db_path):
        print(f"\n⚠️  Base de données '{db_path}' non trouvée.")
        print("Création de données d'exemple...")
        
        # Créer une base d'exemple
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY, size INTEGER)")
        
        # Insérer des données d'exemple suivant approximativement la loi de Benford
        import random
        example_sizes = []
        for _ in range(1000):
            # Générer des nombres suivant une distribution logarithmique
            size = int(10 ** (random.uniform(2, 8)))
            example_sizes.append(size)
            cursor.execute("INSERT INTO files (size) VALUES (?)", (size,))
        
        conn.commit()
        conn.close()
        print(f"✓ Base d'exemple créée avec {len(example_sizes)} entrées")
    
    # Récupérer les tailles
    print(f"\nLecture de la base : {db_path}")
    sizes = get_file_sizes_from_sqlite(db_path)
    print(f"✓ {len(sizes)} tailles de fichiers récupérées")
    
    if len(sizes) == 0:
        print("❌ Aucune donnée trouvée. Vérifiez votre base de données.")
        return
    
    # Statistiques de base
    print(f"\nStatistiques:")
    print(f"  - Minimum : {min(sizes):,} bytes")
    print(f"  - Maximum : {max(sizes):,} bytes")
    print(f"  - Moyenne : {sum(sizes)/len(sizes):,.2f} bytes")
    
    # Calculer les distributions
    print("\nCalcul des distributions...")
    observed_dist, first_digits = calculate_observed_distribution(sizes)
    theoretical_dist = benford_law_two_digits()
    
    print(f"✓ {len(first_digits)} nombres analysés (avec au moins 2 chiffres)")
    
    # Comparer
    chi_square = compare_distributions(observed_dist, theoretical_dist)
    
    # Interprétation du chi-carré
    print("\nInterprétation:")
    if chi_square < 10:
        print("✓ Distribution conforme à la loi de Benford")
    elif chi_square < 20:
        print("⚠️  Écart modéré avec la loi de Benford")
    else:
        print("❌ Distribution significativement différente de la loi de Benford")
    
    # Créer le graphique
    plot_distributions(observed_dist, theoretical_dist)

if __name__ == "__main__":
    main()