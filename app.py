import subprocess
import json
import os
import streamlit as st
import pandas as pd
import re
import sqlite3

from pathlib import Path
from working_DB.initial_scan import scan_folder_and_store
from metadata.Magic_Scan import run_magic_numbers_on_db
from forensic.crude_benefits import analyze_duplicates
from forensic.Benford_distrib import analyze_benford_distribution
from metadata.metadata_router import run_global_metadata_population
from analytics.volume_map import get_folder_volume_df
from forensic.anomalies import run_forensic_analysis
from src.config import DB_PATH          # <-- import du chemin calculé dynamiquement
from src.forensic_detector import run_forensic_scan

if 'selected_tool' not in st.session_state:
    st.session_state['selected_tool'] = None

st.title("Locard.IA AMC 0.2")
st.sidebar.title("🛠️ Tools")


# ---------------------------
# --- FOLDER TO WORK WITH ---
# ---------------------------

folder_toscan = st.text_input(
    "📁 Absolute path to root folder",
    placeholder="/Users/nom/Documents/mon_dossier"
)

if st.button("Confirm."):
    if folder_toscan and Path(folder_toscan).exists():
        # Sauvegarder dans session_state
        st.session_state['dossier_cible'] = folder_toscan
        
        # Sauvegarder dans un fichier config
        with open('config.json', 'w') as f:
            json.dump({'dossier_cible': folder_toscan}, f)
        
        st.success(f"✅ Dossier enregistré : {folder_toscan}")
    else:
        st.error("❌ Dossier invalide ou inexistant")

# ---------------
# --- SIDEBAR ---
# ---------------

if st.sidebar.button("Initialize database"):
    ROOT_DIR = Path(__file__).resolve().parent
    db_init_path = ROOT_DIR / "working_DB" / "db_init.py"
    subprocess.run(["python", str(db_init_path)], check=True)
    st.success("Script exécuté!")


if st.sidebar.button("🔍 Scan sweep"):
    target = st.session_state.get('dossier_cible')
    if target and Path(target).exists():
        scan_folder_and_store(target, str(DB_PATH))
        st.success("Scan terminé et base mise à jour.")
    else:
        st.error("❌ Aucun dossier valide sélectionné pour le scan.")

if st.sidebar.button("🎯 Magic numbers check"):
    st.session_state['selected_tool'] = "Magic numbers check"

if st.sidebar.button("💰 Crude benefits"):
    st.session_state['selected_tool'] = "Crude benefits"

if st.sidebar.button("📉 Benford NNRA"):
    st.session_state['selected_tool'] = "Benford NNRA"

if st.sidebar.button("📊 Volume map"):
    st.session_state['selected_tool'] = "Volume Map"
    
if st.sidebar.button("📋 Populate metadata"):
    st.session_state['selected_tool'] = "Populate metadata"

if st.sidebar.button("🏷️ File labelling"):
    st.session_state['selected_tool'] = "File labelling"

if st.sidebar.button("🔍 Regex Analytics"):
    st.session_state['selected_tool'] = "Regex Analytics"

if st.sidebar.button("📊 Estimate work"):
    st.session_state['selected_tool'] = "Estimate work"

if st.sidebar.button("🧠 LR semantic"):
    st.session_state['selected_tool'] = "LR semantic"

if st.sidebar.button("🎓 HL semantic"):
    st.session_state['selected_tool'] = "HL semantic"

if st.sidebar.button("📝 OCR analysis"):
    st.session_state['selected_tool'] = "OCR analysis"

if st.sidebar.button("📄 Final report"):
    st.session_state['selected_tool'] = "Final report"

if st.sidebar.button("🤖 Classifier"):
    st.session_state['selected_tool'] = "Classifier"

selected_tool = st.session_state.get('selected_tool')


# --------------------------------
# --- SHOW THE SELECTED FOLDER ---
# --------------------------------

if 'dossier_cible' in st.session_state:
    st.info(f"📂 Dossier actuel : {st.session_state['dossier_cible']}")

# -------------------------------
# --- MAGIC NUMBERS CHECK UI ---
# -------------------------------

if selected_tool == "Magic numbers check":
    st.header("🎯 Magic numbers check")

    st.write(
        "Ce module scanne la table `file` de la base SQLite "
        "et remplit la colonne `true_extension` à partir des magic numbers."
    )

    st.code(f"Base utilisée : {DB_PATH}", language="bash")

    if st.button("Lancer le scan Magic Numbers"):
        if not DB_PATH.exists():
            st.error(f"❌ Base SQLite introuvable : {DB_PATH}")
        else:
            progress_bar = st.progress(0.0)
            status_text = st.empty()
            log_area = st.empty()

            logs = []

            def progress_callback(current, total, file_path, ext, desc, error):
                # Mise à jour de la barre de progression
                if total:
                    progress_bar.progress(current / total)
                    status_text.text(f"Traitement {current}/{total}")

                # Log texte
                if file_path is not None:
                    if error:
                        msg = f"[{current}/{total}] ERREUR sur {file_path} : {error}"
                    else:
                        msg = f"[{current}/{total}] {file_path} -> {ext} ({desc})"
                    logs.append(msg)
                    # Afficher seulement les dernières lignes pour rester lisible
                    log_area.text("\n".join(logs[-20:]))

            # Si les chemins stockés dans la colonne `path` sont ABSOLUS, laisse base_dir=None
            run_magic_numbers_on_db(
                db_path=str(DB_PATH),
                base_dir=None,          # ou str(st.session_state['dossier_cible']) si chemins relatifs
                only_missing=True,
                progress_callback=progress_callback,
            )

            st.success("✅ Scan terminé. La colonne `true_extension` a été mise à jour.")

# -------------------------------
# --- CRUDE BENEFITS UI ---
# -------------------------------

if selected_tool == "Crude benefits":
    st.header("💰 Crude benefits")

    st.write(
        "Analyse simple des doublons basée sur les hash SHA256 :\n"
        "- Liste CSV des fichiers dupliqués (même hash_sha256)\n"
        "- Estimation de l'espace disque économisable si on ne conserve qu'un seul exemplaire\n"
        "- Nombre total de fichiers qui pourraient être supprimés"
    )

    st.code(f"Base utilisée : {DB_PATH}", language="bash")

    if st.button("Lancer l'analyse des doublons"):
        if not DB_PATH.exists():
            st.error(f"❌ Base SQLite introuvable : {DB_PATH}")
        else:
            try:
                with st.spinner("Analyse des doublons en cours..."):
                    result = analyze_duplicates(str(DB_PATH))

                if result["groups_count"] == 0:
                    st.success("✅ Aucun doublon détecté (aucun hash_sha256 en double).")
                else:
                    st.success("✅ Analyse terminée.")

                    col1, col2, col3 = st.columns(3)
                    col1.metric("Groupes de doublons", result["groups_count"])
                    col2.metric(
                        "Fichiers potentiellement supprimables",
                        result["removable_files_count"]
                    )
                    col3.metric(
                        "Espace économisable (approx.)",
                        result["wasted_human"]
                    )

                    st.download_button(
                        label="📥 Télécharger la liste CSV des doublons",
                        data=result["csv_bytes"],
                        file_name="duplicate_files_sha256.csv",
                        mime="text/csv"
                    )

            except Exception as e:
                st.error(f"Erreur lors de l'analyse : {e}")

# -------------------------------
# --- FORENSIC AUDIT UI ---
# -------------------------------

if selected_tool == "Forensic Audit":
    st.header("🕵️ Audit Forensique & Anomalies")
    st.write("Analyse heuristique sur 15 indicateurs clés (Spoofing, Timestomping, ZipBombs, Crypto, etc.)")
    
    st.code(f"Base utilisée : {DB_PATH}", language="bash")
    
    if st.button("Lancer l'audit complet"):
        if not DB_PATH.exists():
            st.error(f"❌ Base SQLite introuvable : {DB_PATH}")
        else:
            with st.spinner("Exécution des algorithmes forensiques en cours..."):
                results = run_forensic_analysis(str(DB_PATH))
            
            if "error" in results:
                st.error(results["error"])
            else:
                st.success("✅ Audit terminé. Résultats détaillés ci-dessous.")
                
                # Liste pour l'export consolidé
                export_list = []
                
                # Dictionnaire de mapping pour l'affichage propre
                descriptions = {
                    "spoofing_df": "🚨 Extension Spoofing",
                    "timestomping_df": "⏰ Timestomping (>24h décalage)",
                    "zipbomb_df": "💣 Zip Bombs / Compression Suspecte",
                    "ghost_files_df": "👻 Fichiers Fantômes (Hash Collision)",
                    "secrets_df": "🔑 Secrets Potentiels (Code/Txt)",
                    "encrypted_df": "🔒 Fichiers Chiffrés / Protégés",
                    "unsigned_exe_df": "⚠️ Exécutables Non Signés",
                    "gdpr_heatmap_df": "🛡️ Densité RGPD (Par dossier)",
                    "silent_hours_df": "🌙 Activité Suspecte (Nuit/WE)",
                    "authors_df": "✍️ Auteurs Externes / Multiples",
                    "fakework_df": "⚡ Fake Work / Génération Rapide",
                    "cameras_df": "📷 Empreinte Matérielle (Appareils)",
                    "zombies_df": "🧟 Fichiers Zombies (>3 ans)",
                    "tech_debt_df": "🏚️ Dette Technique (Code)",
                    "geo_df": "🌍 Dispersion Géographique"
                }

                # Affichage des résultats non vides
                count_anomalies = 0
                
                for key, title in descriptions.items():
                    df = results.get(key)
                    if df is not None and not df.empty:
                        count_anomalies += len(df)
                        with st.expander(f"{title} ({len(df)} éléments)", expanded=False):
                            st.dataframe(df)
                        
                        # Préparation Export : On standardise pour concaténer
                        df_export = df.copy()
                        df_export.insert(0, 'Anomaly_Type', title)
                        # On convertit tout en string pour éviter les conflits de types lors du merge
                        df_export = df_export.astype(str)
                        export_list.append(df_export)
                
                if count_anomalies == 0:
                    st.info("Aucune anomalie détectée sur l'ensemble des indicateurs.")
                else:
                    st.warning(f"Total : {count_anomalies} anomalies ou points d'attention détectés.")

                # Bouton Export CSV Unifié
                if export_list:
                    full_report = pd.concat(export_list, ignore_index=True)
                    
                    # Réorganisation intelligente des colonnes pour l'export
                    cols = list(full_report.columns)
                    # On met Anomaly et path au début si existants
                    if 'Anomaly_Type' in cols:
                        cols.insert(0, cols.pop(cols.index('Anomaly_Type')))
                    if 'path' in cols:
                        cols.insert(1, cols.pop(cols.index('path')))
                    
                    full_report = full_report[cols]
                    
                    csv_data = full_report.to_csv(index=False).encode('utf-8')
                    
                    st.download_button(
                        label="📥 Télécharger le Rapport d'Anomalies (CSV)",
                        data=csv_data,
                        file_name="rapport_forensic_complet.csv",
                        mime="text/csv"
                    )



# -------------------------------
# --- POPULATE METADATA UI ---
# -------------------------------

if selected_tool == "Populate metadata":
    st.header("📋 Extraction des Métadonnées")
    st.write(
        "Ce module parcourt tous les fichiers indexés et extrait les métadonnées techniques "
        "(EXIF, ID3, propriétés Office, stats Code, etc.) selon leur type identifié."
    )

    if st.button("Lancer l'extraction"):
        if not DB_PATH.exists():
            st.error("❌ Base de données introuvable. Veuillez lancer l'initialisation et le scan d'abord.")
        else:
            progress_bar = st.progress(0.0)
            status_text = st.empty()
            log_area = st.empty()
            logs = []

            def meta_callback(current, total, filename, status):
                if total > 0:
                    progress_bar.progress(current / total)
                    status_text.text(f"Traitement {current}/{total} : {filename}")
                
                # On log seulement les succès/erreurs, pas les SKIPPED pour réduire le bruit
                if "SUCCESS" in status:
                    logs.append(f"✅ {filename} : {status}")
                elif "ERROR" in status:
                    logs.append(f"❌ {filename} : {status}")
                
                # Affiche les 15 dernières lignes
                if logs:
                    log_area.text("\n".join(logs[-15:]))

            run_global_metadata_population(
                db_path=str(DB_PATH),
                progress_callback=meta_callback
            )
            
            st.success("✅ Extraction des métadonnées terminée.")


# -------------------------------
# --- BENFORD ANALYSIS UI ---
# -------------------------------

if selected_tool == "Benford NNRA":
    st.header("📉 Benford Natural Number Analysis")
    st.write(
        "Cette analyse vérifie si la distribution des tailles de fichiers suit la loi de Benford (sur les 2 premiers chiffres). "
        "Une déviation significative peut indiquer des données générées artificiellement, chiffrées ou altérées."
    )
    
    st.code(f"Base utilisée : {DB_PATH}", language="bash")

    if st.button("Lancer l'analyse Benford"):
        if not DB_PATH.exists():
            st.error(f"❌ Base SQLite introuvable : {DB_PATH}")
        else:
            with st.spinner("Calcul des distributions en cours..."):
                res = analyze_benford_distribution(str(DB_PATH))
            
            if res["success"]:
                st.success("Analyse terminée.")
                
                # 1. Metrics
                col1, col2 = st.columns(2)
                col1.metric("Fichiers analysés", res["file_count"])
                col1.metric("Score Chi-Carré", f"{res['chi_square']:.2f}")
                
                # Interprétation visuelle
                if "✅" in res["interpretation"]:
                    col2.success(res["interpretation"])
                elif "⚠️" in res["interpretation"]:
                    col2.warning(res["interpretation"])
                else:
                    col2.error(res["interpretation"])
                
                # 2. Graphique
                st.pyplot(res["fig"])
                
                # 3. Data Expander
                with st.expander("Voir les données brutes"):
                    st.dataframe(res["dataframe"])
            else:
                st.error(f"Erreur : {res['error']}")

# VOLUMETRIC MAP

if selected_tool == "Volume Map":
    st.header("📊 Cartographie volumétrique")

    if st.button("Calculer la cartographie"):
        df = get_folder_volume_df(str(DB_PATH))

        st.write("Top 30 dossiers par volume :")
        df_top = df.sort_values("total_size_bytes", ascending=False).head(30)
        df_top_display = df_top.assign(
            total_size_gb = df_top["total_size_bytes"] / (1024**3)
        )
        st.dataframe(df_top_display[["folder_path", "file_count", "total_size_gb"]])

        # Export CSV
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Télécharger le CSV complet",
            data=csv_bytes,
            file_name="volume_map.csv",
            mime="text/csv",
        )

# -------------------------------
# --- REGEX ANALYTICS UI ---
# -------------------------------

if selected_tool == "Regex Analytics":
    st.header("🔍 Analyse Regex Forensique (16 catégories)")

    st.write(
        "Ce module scanne tous les fichiers texte (code, logs, configs, etc.) "
        "à la recherche de données sensibles comme :\n"
        "- NSS, Carte d’identité, Téléphone\n"
        "- Mot de passe, Clé API, Carte bancaire\n"
        "- URL internes, Fichiers temporaires, Commentaires sensibles\n"
        "\n"
        "Les résultats sont enregistrés dans la base et affichés ci-dessous."
    )

    st.code(f"Base utilisée : {DB_PATH}", language="bash")

    if st.button("Lancer l'analyse Regex"):
        with st.spinner("Analyse en cours... (16 catégories de regex)"):
            try:
                # Importer le module de détection
                from src.forensic_detector import run_forensic_scan

                # Exécuter le scan
                run_forensic_scan(str(DB_PATH))

                # Récupérer les résultats depuis la base
                conn = sqlite3.connect(str(DB_PATH))
                df = pd.read_sql_query("""
                    SELECT 
                        f.path AS fichier,
                        d.category AS catégorie,
                        d.value AS valeur,
                        d.detected_at AS date_detection
                    FROM file_sensitivity_detection d
                    JOIN file f ON d.file_id = f.id
                    ORDER BY d.detected_at DESC
                """, conn)
                conn.close()

                # Afficher le tableau
                st.success(f"✅ Analyse terminée. {len(df)} détections trouvées.")

                if not df.empty:
                    st.dataframe(df)

                    # Export CSV
                    csv_data = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Télécharger les résultats (CSV)",
                        data=csv_data,
                        file_name="regex_detection_results.csv",
                        mime="text/csv"
                    )

                    # Statistiques par catégorie
                    st.subheader("📊 Résumé par catégorie")
                    stats = df['catégorie'].value_counts()
                    st.bar_chart(stats)

                else:
                    st.info("Aucune donnée sensible détectée.")

            except Exception as e:
                st.error(f"❌ Erreur lors de l'analyse : {e}")