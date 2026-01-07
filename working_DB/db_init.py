#!/usr/bin/env python3
import sqlite3
from pathlib import Path

# Chemin absolu a modifier
DB_PATH = "/Users/maxime/DEV/CommandoAI/working_DB/project_index.db"

SCHEMA_SQL = """
----------------------------------------------------------------------
-- 0. FOLDERS (unités logiques de scoring)
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS folder (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    path            TEXT NOT NULL UNIQUE,          -- chemin absolu / logique
    parent_id       INTEGER REFERENCES folder(id) ON DELETE CASCADE,
    files_subcount  INTEGER,
    files_totcount  INTEGER,
    personnal_fold  INTEGER,
    shared_fold     INTEGER,
    app_fold        INTEGER
);

CREATE INDEX IF NOT EXISTS idx_folder_parent_id ON folder(parent_id);
CREATE INDEX IF NOT EXISTS idx_folder_path       ON folder(path);

----------------------------------------------------------------------
-- 1. MAIN FILE TABLE
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS file (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Lien logique vers le dossier
    folder_id       INTEGER
                    REFERENCES folder(id) ON DELETE CASCADE,

    -- Identité
    path            TEXT NOT NULL UNIQUE,
    size_bytes      INTEGER,
    mtime           INTEGER,              -- timestamp (epoch)

    -- Type / format
    decl_extension  TEXT,
    true_extension  TEXT,
    ext_family      TEXT,                 -- 'image', 'audio', 'video', ...
    mime_detected   TEXT,

    -- Hash
    hash_sha256     TEXT,

    -- Suivi interne
    ent_created_at  TEXT DEFAULT (datetime('now')),
    updated_at      TEXT,
    last_update     TEXT,

    -- Sémantique métier (héritée de ton schéma)
    doc_function    TEXT,
    doc_family      INTEGER,            -- 0 : undef , 1 : HRM , 2 : Finance&accounting , 3 : Core Job , 4 : legal , 5: Comm&Marketing , 6: IT , 7: logistics , 8: Sales , 99 : multiple 

    -- Niveau d'analyse à prévoir
    llm_analys_stat INTEGER,            -- 0 : no need semantic analysis    1: low    2: medium    3: high
    llm_analys_done TEXT,               -- LLM scan date
    cc_status       INTEGER,            -- 0 : no need OCR                  1: low    2: medium    3: high

    -- Flags
    gdpr_risk       INTEGER,            -- 0 : no       1 : yes (flag binaire)
    hasto_skip      INTEGER,            -- 0 : no       1 : yes

    ------------------------------------------------------------------
    -- Job family.  See job_referential.txt
    ------------------------------------------------------------------
    main_job_fam    INTEGER,
    job_subtype     INTEGER,

    ------------------------------------------------------------------
    -- Scores / poids par fichier (pour agrégation dossier)
    ------------------------------------------------------------------

    rgpd_score_file      REAL,         -- [0,1] risque RGPD estimé
    business_criticality REAL,         -- [0,1] customer rules defined prior to analysis
    legal_risk_weight    REAL,         -- [0,1] analysis of potential legal contracting 
    finance_risk_weight  REAL,         -- [0,1] analysis of potential financial risks
    combined_score       REAL          -- [0,1] Combined score
);

CREATE INDEX IF NOT EXISTS idx_file_folder_id      ON file(folder_id);
CREATE INDEX IF NOT EXISTS idx_file_ext_family     ON file(ext_family);
CREATE INDEX IF NOT EXISTS idx_file_decl_extension ON file(decl_extension);
CREATE INDEX IF NOT EXISTS idx_file_true_extension ON file(true_extension);
CREATE INDEX IF NOT EXISTS idx_file_doc_family     ON file(doc_family);

----------------------------------------------------------------------
-- 2. Images
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS file_image_metadata (
    file_id                 INTEGER PRIMARY KEY
                            REFERENCES file(id) ON DELETE CASCADE,

    -- Type / format / dimensions
    image_type              TEXT,       -- 'raster' / 'vector'
    format                  TEXT,       -- 'jpeg', 'png', 'svg', ...
    width_px                INTEGER,
    height_px               INTEGER,
    dpi_x                   REAL,
    dpi_y                   REAL,
    bits_per_channel        INTEGER,
    bits_per_pixel          INTEGER,
    color_space             TEXT,       -- 'sRGB', 'AdobeRGB', ...
    has_alpha               INTEGER,    -- 0/1
    orientation             INTEGER,    -- EXIF 1..8
    has_embedded_thumbnail  INTEGER,    -- 0/1

    -- Main EXIF metadata 
    exif_datetime_original  TEXT,       -- ISO8601
    camera_make             TEXT,
    camera_model            TEXT,
    lens_model              TEXT,
    focal_length_mm         REAL,
    aperture_f              REAL,
    exposure_time_s         REAL,
    iso                     INTEGER,
    flash_used              INTEGER,    -- 0/1

    -- GPS data
    gps_lat                 REAL,
    gps_lon                 REAL,
    gps_alt                 REAL,

    -- Autors / content
    author                  TEXT,
    title                   TEXT,
    description             TEXT,
    keywords                TEXT,       -- ex: 'tag1,tag2'
    copyright               TEXT,
    software                TEXT        -- build with / modified with
);

----------------------------------------------------------------------
-- 3. Audio
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS file_audio_metadata (
    file_id             INTEGER PRIMARY KEY
                        REFERENCES file(id) ON DELETE CASCADE,

    -- Technical data
    container_format    TEXT,       -- 'mp3', 'wav', 'flac', ...
    audio_codec         TEXT,       -- 'MP3', 'AAC', 'Opus', ...
    duration_sec        REAL,
    bitrate_kbps        REAL,
    sample_rate_hz      INTEGER,
    channels            INTEGER,
    is_vbr              INTEGER,    -- 0/1
    loudness_lufs       REAL,
    peak_db             REAL,

    -- Tags 
    title               TEXT,
    artist              TEXT,
    album               TEXT,
    album_artist        TEXT,
    composer            TEXT,
    genre               TEXT,
    track_number        INTEGER,
    track_total         INTEGER,
    disc_number         INTEGER,
    disc_total          INTEGER,
    year                INTEGER,
    date                TEXT,       -- date complète si dispo

    -- Misc
    has_lyrics          INTEGER,    -- 0/1
    lyrics_language     TEXT,
    has_cover           INTEGER,    -- 0/1
    label               TEXT,
    copyright           TEXT,
    publisher           TEXT,
    isrc                TEXT,
    encoder             TEXT
);

----------------------------------------------------------------------
-- 4. Videos
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS file_video_metadata (
    file_id                 INTEGER PRIMARY KEY
                            REFERENCES file(id) ON DELETE CASCADE,

    -- Containers & streams
    container_format        TEXT,       -- 'mp4', 'mkv', ...
    video_codec             TEXT,
    audio_codec             TEXT,
    duration_sec            REAL,

    -- Video
    width_px                INTEGER,
    height_px               INTEGER,
    fps                     REAL,
    video_bitrate_kbps      REAL,
    aspect_ratio            TEXT,       -- '16:9', '4:3', ...
    is_interlaced           INTEGER,    -- 0/1
    color_space             TEXT,       -- 'BT.709', ...
    is_hdr                  INTEGER,    -- 0/1
    profile                 TEXT,
    level                   TEXT,

    -- Audio
    audio_bitrate_kbps      REAL,
    audio_sample_rate_hz    INTEGER,
    audio_channels          INTEGER,
    audio_lang_main         TEXT,       -- 'fr', 'en', ...

    -- Subtitles
    subtitle_track_count    INTEGER,
    subtitle_languages      TEXT,       -- ex: 'fr,en,es'
    subtitle_forced_langs   TEXT,

    -- Global informations
    title                   TEXT,
    show_name               TEXT,
    season_number           INTEGER,
    episode_number          INTEGER,
    year                    INTEGER,
    director                TEXT,

    -- Misc
    has_chapters            INTEGER,    -- 0/1
    chapter_count           INTEGER,
    creation_time           TEXT,       -- ISO8601
    encoder                 TEXT
);

----------------------------------------------------------------------
-- 5. Office (Word/Excel/PPT/ODF)
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS file_office_metadata (
    file_id                 INTEGER PRIMARY KEY
                            REFERENCES file(id) ON DELETE CASCADE,

    -- Type & structure
    office_type             TEXT,       -- 'word', 'excel', 'powerpoint', ...
    page_count              INTEGER,
    slide_count             INTEGER,
    sheet_count             INTEGER,
    word_count              INTEGER,
    char_count              INTEGER,
    wrdstruct_type          INTEGER,    -- 0 : small document       1 : mid document    2 : dense document
    multiple_doc            INTEGER,    -- this document has multiple subjects that can't be clearly define by family or function

    -- Common metadata
    title                   TEXT,
    subject                 TEXT,
    keywords                TEXT,
    description             TEXT,
    language                TEXT,
    author                  TEXT,
    last_modified_by        TEXT,
    company                 TEXT,

    -- History / versions
    created_at              TEXT,       -- ISO8601
    modified_at             TEXT,       -- ISO8601
    printed_at              TEXT,       -- ISO8601
    revision_number         TEXT,

    -- Specs Office
    has_macros              INTEGER,    -- 0/1
    template_name           TEXT,
    total_editing_time_sec  INTEGER,

    -- Semantic Analysis
    Exerpt_hund             TEXT,
    Exerpt_thou             TEXT,
    Exerpt_full             TEXT
);

----------------------------------------------------------------------
-- 6. PDF
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS file_pdf_metadata (
    file_id             INTEGER PRIMARY KEY
                        REFERENCES file(id) ON DELETE CASCADE,

    -- Document Structure
    page_count          INTEGER,
    has_text            INTEGER,    -- 0/1
    has_images          INTEGER,    -- 0/1
    has_forms           INTEGER,    -- 0/1
    has_signatures      INTEGER,    -- 0/1
    is_encrypted        INTEGER,    -- 0/1

    -- Metadata
    title               TEXT,
    author              TEXT,
    subject             TEXT,
    keywords            TEXT,
    creator             TEXT,
    producer            TEXT,
    language            TEXT,

    -- Dates / norms
    created_at          TEXT,       -- ISO8601
    modified_at         TEXT,       -- ISO8601
    pdf_version         TEXT,
    pdf_conformance     TEXT,        -- ex: 'PDF/A-1b'

    -- Document quality
    is_blurred          INTEGER,
    is_llavaocr_req     INTEGER,

    -- Semantic Analysis
    Exerpt_hund             TEXT,
    Exerpt_thou             TEXT,
    Exerpt_full             TEXT
  
);

----------------------------------------------------------------------
-- 7. Texte
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS file_text_metadata (
    file_id             INTEGER PRIMARY KEY
                        REFERENCES file(id) ON DELETE CASCADE,

    -- Encoding
    encoding            TEXT,
    has_bom             INTEGER,    -- 0/1

    -- Statistics
    line_count          INTEGER,
    word_count          INTEGER,
    char_count          INTEGER,
    avg_line_length     REAL,

    -- Type & language
    detected_text_type  TEXT,       -- 'plain', 'json', 'xml', ...
    language            TEXT,

    -- Structure type boolean trigger
    is_json_valid       INTEGER,    -- 0/1
    is_xml_valid        INTEGER,    -- 0/1
    is_yaml_valid       INTEGER,    -- 0/1

    -- Heuristics
    has_urls            INTEGER,    -- 0/1
    has_emails          INTEGER,    -- 0/1
    has_ips             INTEGER,    -- 0/1
    has_secrets         INTEGER,    -- 0/1
    comment_ratio       REAL,
    license_name        TEXT,

    -- Semantic Analysis
    Exerpt_hund             TEXT,
    Exerpt_thou             TEXT,
    Exerpt_full             TEXT
    
);

----------------------------------------------------------------------
-- 8. Archives
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS file_archive_metadata (
    file_id                 INTEGER PRIMARY KEY
                            REFERENCES file(id) ON DELETE CASCADE,

    -- File type and size
    archive_format          TEXT,       -- 'zip', 'rar', ...
    compressed_size         INTEGER,
    total_uncompressed_size INTEGER,
    compression_ratio       REAL,

    -- Datas inside
    file_count              INTEGER,
    dir_count               INTEGER,
    folder_count            INTEGER,
    largest_file_size       INTEGER,
    has_executables         INTEGER,    -- 0/1

    -- Semantic Analysis
    Exerpt_hund             TEXT,
    Exerpt_thou             TEXT,
    Exerpt_full             TEXT,
    
    -- Security and structures
    is_encrypted            INTEGER,    -- 0/1
    is_password_protected   INTEGER,    -- 0/1
    is_solid                INTEGER,    -- 0/1
    is_multivolume          INTEGER,    -- 0/1
    volume_index            INTEGER,

    -- Historic 
    oldest_entry_time       TEXT,       -- ISO8601
    newest_entry_time       TEXT,       -- ISO8601
    top_level_entry_count   INTEGER
);

----------------------------------------------------------------------
-- 9. Images disque
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS file_disk_image_metadata (
    file_id             INTEGER PRIMARY KEY
                        REFERENCES file(id) ON DELETE CASCADE,

    -- Format
    disk_image_format   TEXT,       -- 'iso', 'dmg', 'vhd', ...
    size_bytes          INTEGER,    -- taille de l'image
    is_bootable         INTEGER,    -- 0/1

    -- Partitioning / FS
    partition_count     INTEGER,
    filesystem_types    TEXT,       -- ex: 'ntfs,fat32,ext4'
    has_mbr             INTEGER,    -- 0/1
    has_gpt             INTEGER,    -- 0/1

    -- OS / role
    os_guess            TEXT,       -- 'windows_install', 'linux_live', ...
    volume_label        TEXT
);

----------------------------------------------------------------------
-- 10. EXE
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS file_exe_metadata (
    file_id             INTEGER PRIMARY KEY
                        REFERENCES file(id) ON DELETE CASCADE,

    -- Technical info extracted by pefile
    architecture        TEXT,
    compile_timestamp   TEXT,
    entry_point         TEXT,
    subsystem           TEXT,
    is_signed           INTEGER,    -- 0/1
    
    -- Structure stats
    section_count       INTEGER,
    import_count        INTEGER,
    export_count        INTEGER,

    -- Semantic Analysis (Technical Summary)
    Exerpt_hund         TEXT,
    Exerpt_thou         TEXT,
    Exerpt_full         TEXT
);

----------------------------------------------------------------------
-- 11. Source code
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS file_code_metadata (
    file_id                 INTEGER PRIMARY KEY
                            REFERENCES file(id) ON DELETE CASCADE,

    -- Language / encoding
    language                TEXT,   -- 'python', 'java', ...
    encoding                TEXT,

  -- Statistics
    line_count              INTEGER,
    lines_code              INTEGER,
    lines_comment           INTEGER,  -- Renommé pour correspondre au script python
    comment_ratio           REAL,
    lines_total             INTEGER,
    lines_empty             INTEGER,  -- Renommé pour correspondre au script python

    -- Structure
    function_count          INTEGER,
    class_count             INTEGER,
    import_count            INTEGER,
    todo_count              INTEGER,

    -- Tech / frameworks
    main_frameworks         TEXT,   -- ex: 'django,pytest'
    has_tests               INTEGER,    -- 0/1
    has_main_entrypoint     INTEGER,    -- 0/1

    -- Style / licence
    license_name            TEXT,
    indent_style            TEXT,   -- 'spaces', 'tabs'
    indent_size             INTEGER,

   -- Security
    has_secrets             INTEGER,    -- 0/1

    -- Semantic Analysis
    Exerpt_hund             TEXT,
    Exerpt_thou             TEXT,
    Exerpt_full             TEXT
);

----------------------------------------------------------------------
-- 12. Data Bases
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS file_database_metadata (
    file_id                 INTEGER PRIMARY KEY
                            REFERENCES file(id) ON DELETE CASCADE,

    -- General specs
    engine                  TEXT,   -- 'sqlite', 'access', 'parquet', ...
    schema_version          TEXT,

    -- Structure
    table_count             INTEGER,
    view_count              INTEGER,
    index_count             INTEGER,
    trigger_count           INTEGER,
    stored_proc_count       INTEGER,

    -- Volume
    row_count_estimate      INTEGER,
    largest_table_name      TEXT,
    largest_table_row_count INTEGER,

    -- Constraints
    has_foreign_keys        INTEGER,    -- 0/1
    is_encrypted            INTEGER     -- 0/1
);

----------------------------------------------------------------------
-- 13. Tabular datas (CSV, XLS...)
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS file_data_metadata (
    file_id                 INTEGER PRIMARY KEY
                            REFERENCES file(id) ON DELETE CASCADE,

    -- Format
    data_format             TEXT,   -- 'csv', 'tsv', 'xlsx', 'parquet', ...
    encoding                TEXT,
    delimiter               TEXT,
    has_header              INTEGER,    -- 0/1

    -- Dimensions
    row_count               INTEGER,
    column_count            INTEGER,

    -- Column types
    numeric_col_count       INTEGER,
    categorical_col_count   INTEGER,
    datetime_col_count      INTEGER,
    text_col_count          INTEGER,

    -- Quality
    has_missing_values      INTEGER,    -- 0/1
    missing_value_ratio     REAL,
    has_duplicate_rows      INTEGER,    -- 0/1

    -- Others
    estimated_memory_bytes  INTEGER,
    text_language           TEXT,
    has_identifiers         INTEGER     -- 0/1 (ID/email détectés)
);

----------------------------------------------------------------------
-- 14. 3D / CAO / SIG
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS file_3d_metadata (
    file_id         INTEGER PRIMARY KEY
                    REFERENCES file(id) ON DELETE CASCADE,

    -- Format
    format          TEXT,       -- 'obj', 'fbx', 'stl', 'dwg', 'shp', ...
    unit_scale      REAL,
    coord_system    TEXT,       -- 'Y_UP', 'Z_UP', ...

    -- Complexity
    mesh_count      INTEGER,
    vertex_count    INTEGER,
    face_count      INTEGER,

    -- Materials and textures
    material_count  INTEGER,
    texture_count   INTEGER,

    -- Animation & scenarisation
    has_animations  INTEGER,    -- 0/1
    frame_count     INTEGER,
    has_skeleton    INTEGER,    -- 0/1
    has_cameras     INTEGER,    -- 0/1
    has_lights      INTEGER,    -- 0/1

    -- Bounding box
    bbox_min_x      REAL,
    bbox_min_y      REAL,
    bbox_min_z      REAL,
    bbox_max_x      REAL,
    bbox_max_y      REAL,
    bbox_max_z      REAL,

    -- Autor
    author          TEXT,
    tool            TEXT,
    created_at      TEXT        -- ISO8601
);

----------------------------------------------------------------------
-- 15. Fonts
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS file_font_metadata (
    file_id             INTEGER PRIMARY KEY
                        REFERENCES file(id) ON DELETE CASCADE,

    -- Names
    family_name         TEXT,
    subfamily_name      TEXT,
    full_name           TEXT,
    postscript_name     TEXT,

    -- Style
    style_category      TEXT,   -- 'serif', 'sans-serif', 'mono', ...
    weight              INTEGER,    -- 100..900
    width               TEXT,   -- 'condensed', 'normal', 'expanded'
    is_italic           INTEGER,    -- 0/1
    is_bold             INTEGER,    -- 0/1

    -- Covers
    glyph_count         INTEGER,
    unicode_ranges      TEXT,   -- ex: 'Latin,Latin-1,Greek'

    -- Metrics
    units_per_em        INTEGER,
    ascent              INTEGER,
    descent             INTEGER,

    -- Licence
    designer            TEXT,
    foundry             TEXT,
    license_type        TEXT,   -- 'OFL', 'commercial', ...

    -- Variable fonts
    is_variable         INTEGER,    -- 0/1
    variation_axis_count INTEGER
);

----------------------------------------------------------------------
-- 16. Projects (Cut, audio, graphics...)
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS file_project_metadata (
    file_id                INTEGER PRIMARY KEY
                           REFERENCES file(id) ON DELETE CASCADE,

    -- Application
    app_name               TEXT,   -- 'Premiere', 'AfterEffects', ...
    app_version            TEXT,
    project_type           TEXT,   -- 'video_edit', 'audio_edit', ...

    -- Main Timeline 
    main_duration_sec      REAL,
    sequence_count         INTEGER,
    track_count            INTEGER,

    -- Medias
    media_ref_count        INTEGER,
    missing_media_count    INTEGER,

    -- Output parameters 
    output_width_px        INTEGER,
    output_height_px       INTEGER,
    output_fps             REAL,
    audio_channels         INTEGER,
    audio_sample_rate_hz   INTEGER,
    color_space            TEXT,

    -- Project Information
    created_at             TEXT,   -- ISO8601
    modified_at            TEXT,   -- ISO8601
    author                 TEXT,
    internal_revision      TEXT,
    autosave_enabled       INTEGER,    -- 0/1
    backup_file_count      INTEGER
);
"""


def init_db(db_path: str = DB_PATH) -> None:
    """Create the SQL Tables if they don't exist."""
    db_path = str(db_path)
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        # PRAGMA de base
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")

        # Création des tables
        conn.executescript(SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    init_db()
    print(f"Base initialisée : {DB_PATH}")