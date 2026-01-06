import subprocess
import json
import os
import streamlit as st

from pathlib import Path
from working_DB.initial_scan import scan_folder_and_store
from metadata.Magic_Scan import run_magic_numbers_on_db
from forensic.crude_benefits import analyze_duplicates
from forensic.Benford_distrib import analyze_benford_distribution
from metadata.metadata_router import run_global_metadata_population

if 'selected_tool' not in st.session_state:
    st.session_state['selected_tool'] = None

st.title("Locard.IA AMC 0.2")
st.sidebar.title("🛠️ Tools")

ROOT_DIR = Path(__file__).resolve().parent
DB_PATH = ROOT_DIR / "working_DB" / "project_index.db"

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

if st.sidebar.button("📋 Populate metadata"):
    st.session_state['selected_tool'] = "Populate metadata"

if st.sidebar.button("🏷️ File labelling"):
    st.session_state['selected_tool'] = "File labelling"

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