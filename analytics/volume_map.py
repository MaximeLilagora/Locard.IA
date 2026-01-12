#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Cartographie volumétrique des dossiers à partir de project_index.db

- Agrège la taille et le nombre de fichiers par dossier (table folder + file)
- Exporte les résultats en CSV
- Génère un graphique (barres ou treemap) exportable en PNG
"""

import sqlite3
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


def get_folder_volume_df(db_path: str) -> pd.DataFrame:
    """
    Retourne un DataFrame avec, pour chaque dossier :
        - folder_id
        - folder_path
        - depth (profondeur dans l'arborescence)
        - file_count
        - total_size_bytes
    """
    conn = sqlite3.connect(db_path)
    try:
        query = """
        SELECT
            f.id              AS folder_id,
            f.path            AS folder_path,
            -- estimation de la profondeur (nb de / dans le chemin)
            (LENGTH(f.path) - LENGTH(REPLACE(f.path, '/', ''))) AS depth,
            COUNT(fi.id)      AS file_count,
            COALESCE(SUM(fi.size_bytes), 0) AS total_size_bytes
        FROM folder f
        LEFT JOIN file fi ON fi.folder_id = f.id
        GROUP BY f.id, f.path
        ORDER BY total_size_bytes DESC;
        """
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    return df


def export_volume_csv(df: pd.DataFrame, output_csv: str | Path) -> None:
    """
    Exporte le DataFrame en CSV.
    """
    output_csv = Path(output_csv)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False, encoding="utf-8")


def plot_top_folders_bar(
    df: pd.DataFrame,
    top_n: int = 20,
    output_png: str | Path | None = None,
) -> None:
    """
    Génère un graphique en barres des N dossiers les plus volumineux.
    - df : DataFrame issu de get_folder_volume_df
    - top_n : nombre de dossiers à afficher
    - output_png : si renseigné, sauvegarde le graphique en PNG
    """
    # On prend les top_n dossiers par taille
    df_top = df.sort_values("total_size_bytes", ascending=False).head(top_n)

    # Conversion en Go pour lisibilité
    df_top = df_top.copy()
    df_top["total_size_gb"] = df_top["total_size_bytes"] / (1024**3)

    plt.figure(figsize=(12, 6))
    plt.barh(
        df_top["folder_path"],
        df_top["total_size_gb"],
        color="#4C72B0",
    )
    plt.xlabel("Taille (Go)")
    plt.ylabel("Dossier")
    plt.title(f"Top {top_n} dossiers par volume (Go)")
    plt.gca().invert_yaxis()  # pour avoir le plus gros en haut
    plt.tight_layout()

    if output_png is not None:
        output_png = Path(output_png)
        output_png.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_png, dpi=150)

    # Si tu veux juste générer le fichier sans afficher, ne fais pas plt.show()
    # plt.show()
    plt.close()


def main():
    """
    Usage simple en ligne de commande :

    python volume_map.py /chemin/vers/project_index.db \
                         --csv output/volume_map.csv \
                         --png output/volume_top20.png
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Cartographie volumétrique des dossiers à partir de project_index.db"
    )
    parser.add_argument(
        "db_path",
        help="Chemin vers la base SQLite project_index.db",
    )
    parser.add_argument(
        "--csv",
        dest="csv_path",
        default="volume_map.csv",
        help="Chemin du fichier CSV de sortie (par défaut: volume_map.csv)",
    )
    parser.add_argument(
        "--png",
        dest="png_path",
        default="volume_top20.png",
        help="Chemin de l'image PNG pour le graphique (par défaut: volume_top20.png)",
    )
    parser.add_argument(
        "--top",
        dest="top_n",
        type=int,
        default=20,
        help="Nombre de dossiers à afficher dans le graphique (par défaut: 20)",
    )

    args = parser.parse_args()

    db_path = args.db_path
    csv_path = args.csv_path
    png_path = args.png_path
    top_n = args.top_n

    print(f"[+] Chargement des données depuis : {db_path}")
    df = get_folder_volume_df(db_path)

    print(f"[+] {len(df)} dossiers trouvés.")
    print(f"[+] Export CSV -> {csv_path}")
    export_volume_csv(df, csv_path)

    print(f"[+] Génération du graphique Top {top_n} -> {png_path}")
    plot_top_folders_bar(df, top_n=top_n, output_png=png_path)

    print("[+] Terminé.")


if __name__ == "__main__":
    main()
