# -------------------------------------------
# ---- PHASE ONE : Global initialization ----
# -------------------------------------------
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import threading
import working_DB.db_init as db_init     # Checking DB availability threw db.init call
import os
import hashlib
import json
import sqlite3
import requests
import filetype

# ------------------------------------------------
# ---- PHASE TWO : Query usr / which folder ? ----
# ------------------------------------------------
# BE POLITE ! Ask the user, hey dude, what do you want to scan ?

# ----------------------------------------
# ---- PHASE THREE : Initial scanning ----
# ----------------------------------------
# Performing a global scanner to populate the first fields of Id Table

# ---------------------------------------------------------------------------------------
# ---- PHASE FOUR : First analysis : doubles, corrupted and extension identification ----
# ---------------------------------------------------------------------------------------
# Using SHA256 Hash id for Doubles identification
# Using Magic numbers for true extension id
# Corrupted files : I have to find a method
# Database refining and corrections

# ----------------------------------------------------------------------------
# ---- PHASE FIVE : First report : Crude benefits and low level analytics ----
# ----------------------------------------------------------------------------
# Produce a first report drafted to estimate the crude immediate gain - storage space - and a first view on global data structure composition

# ------------------------------------------------------------------------------------------------
# ---- PHASE SIX : Metadata scrapping : Populating specific field according to files families ----
# ------------------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------------------
# --- PHASE SEVEN : Second level analysis - Preparing labelling files for further AI semantic / OCR inference ---
# ---------------------------------------------------------------------------------------------------------------
# !!!!!!!!!!! THIS IS WHERE THE REAL DEAL STARTS. THIS SOULD BE CONSIDERED AS THE MOST IMPORTANT LOGIC EFFORT TO IMPLEMENT EFFICIENT ALGORITHMS TO SPARE CRITICAL INFERENCE 
# RESSOURCES. Informations from each metadata collection must be carefully scrutined, with as final goal to tag each file with sufficent powered AI Statut.

# -----------------------------------------------------------------------------------------------
# --- PHASE EIGHT : Second report - Estimating time and ressources costs for global inference ---
# -----------------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------
# --- PHASE NINE : LLM low level inference classification with ministral-3:3B ---
# -------------------------------------------------------------------------------
# The goal is to label by semantic the primal nature of each document by function and nature :
# Functions for organization : Flexible by nature but in modern organization, there are most likely those
# 1. The Management Function (Steering) : Executive Management / General Management
# 2. Operational Functions (Core Business) : Production / Operations, Research and Development (R&D), Logistics / Supply Chain, Sales, Marketing
# 3. Support Functions : Finance and Accounting, Management Control (FP&A), Treasury, Human Resources (HR), Purchasing / Procurement, Information Systems (IT), Legal
# 4. General Services / Facilities Management: Manages buildings, maintenance, premises security, and office equipment.
#

# The nature of document can be defined as follow
# Here is the list of document types found in modern organizations, categorized by families (functional categories).
# 1. Corporate & Legal Family
# These documents define the existence, governance, and compliance of the company.
# Constitutive Documents: Articles of association (bylaws), certificate of incorporation.
# Contracts & Agreements: Client/supplier contracts, Non-Disclosure Agreements (NDA), partnership agreements, Terms and Conditions (T&C).
# Compliance: GDPR registries, ethics charters, audit reports, risk assessments.
# Intellectual Property: Patents, trademark registrations, copyright filings.

# 2. Financial & Accounting Family
# Documents related to money management, reporting, and fiscal obligations.
# Transactional: Quotes (estimates), purchase orders (PO), invoices (sales & purchase), delivery notes, expense reports.
# Accounting Records: General ledger, bank statements, tax returns.
# Financial Statements: Balance sheet, income statement (P&L), cash flow statement.
# Planning: Budgets, financial forecasts, investment plans.

# 3. Human Resources (HR) Family
# Documents concerning the workforce and employee lifecycle.
# Recruitment: Job descriptions, CVs/Resumes, cover letters, offer letters.
# Employment: Employment contracts, amendments, pay slips (payroll), timesheets.
# Development: Performance reviews (appraisals), training materials, onboarding handbooks.
# Internal Policies: Internal regulations, code of conduct, leave requests.

# 4. Operational & Project Family
# Documents used to execute daily work and manage projects.
# Project Management: Project charters, roadmaps, Gantt charts, meeting minutes/notes.
# Technical: Specifications (specs), blueprints/plans, technical manuals, API documentation.
# Process & Quality: Standard Operating Procedures (SOPs), quality manuals, incident reports.
# Logistics: Inventory lists, shipping manifests, customs declarations.

# 5. Sales & Marketing Family
# Documents used to attract customers and generate revenue.
# Sales Materials: Pitch decks (presentations), product brochures, price lists, proposals (RFP responses).
# Marketing Content: White papers, case studies, blog posts, newsletters, press releases.
# Customer Data: CRM records, customer feedback forms, survey results.

# 6. IT & Data Family
# The digital backbone of the organization.
# Architecture: Network diagrams, database schemas, disaster recovery plans.
# Security: Access logs, security policies, penetration test reports.
# Software Development: User stories, bug reports (e.g., Jira tickets), release notes.

# Modern Format Types
# In modern organizations, the "nature" of a document is also defined by its interactivity:
# Collaborative Wikis: (e.g., Notion, Confluence) – Replace static manuals.
# Live Dashboards: (e.g., PowerBI, Tableau) – Replace static Excel reports.
# Asynchronous Video: (e.g., Loom, recordings) – Replace long emails or meeting minutes.