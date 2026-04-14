# Visual Condition Data Processing Pipeline

## Overview

This pipeline processes **visual condition inspection data** to produce **asset-level health metrics**, including a **Weighted Condition Score (WCS)** suitable for engineering assessment, reporting, and downstream systems (e.g. ETIP).

The workflow ingests raw inspection exports, validates their integrity, enriches them with importance weighting, and generates structured outputs that reflect the current condition of assets based on visual inspection outcomes.

---

## Objectives

- Standardise raw visual inspection data
- Validate inspection integrity and script versions
- Apply importance weighting to condition responses
- Calculate asset-level **Weighted Condition Scores (WCS)**
- Prepare outputs for **asset health reporting** and **ETIP ingestion**
- Provide auditability through logging and automated alerts

---

## Inputs

The pipeline expects the following Excel inputs:

| File | Purpose |
|-----|--------|
| **Visual inspections Results (E0309B).xlsx** | Raw visual inspection responses |
| **Importance_Scores.xlsx** | Importance weighting for each script/question |
| **Script_Versions.xlsx** | Expected maximum script versions |

> File paths are currently hard-coded and should be updated if the source location changes.

---

## High‑Level Pipeline Flow

1. Import and log input data  
2. Clean and standardise inspection records  
3. Validate scripts and inspection metadata  
4. Enrich inspection data (availability, deterioration, civil items)  
5. Calculate question‑level WCS elements  
6. Aggregate to asset‑level WCS  
7. Categorise asset health bands  
8. Prepare ETIP‑ready output tables  
9. Export results and notify on completion  

---

## Detailed Processing Steps

### 1. Data Import
- Uses `pandas` for data handling
- Logging configured to both console and `log.txt`
- All column names trimmed to avoid whitespace errors

---

### 2. Data Cleaning & Standardisation
- Removes blank rows above header definitions
- Sets correct column headers dynamically
- Standardises data types:
  - Dates → datetime
  - Numeric scores → numeric
  - Identifiers → string

---

### 3. Data Validation

#### Script Version Validation
- Extracts maximum script versions observed in inspection data
- Compares against expected versions
- Flags and removes inspections using higher‑than‑expected script versions
- Exports flagged rows to `problematic_scripts.csv`

#### Inspection Date Consistency
- Verifies that each `Script Activity ID` maps to a single inspection date
- On failure:
  - Logs the issue
  - Sends an automated Outlook email
  - Halts execution

---

### 4. Data Enrichment

Enrichment includes:

- Identification of:
  - **Unavailable assets**
  - **Assets with no deterioration reported**
- Engineering‑specific tagging:
  - Civil items vs civil photos
  - 5‑character site codes
- Preservation of supporting artefacts (photos, document links)

---

### 5. Weighted Condition Score (WCS)

#### Question‑Level Scoring
Each inspection response is assigned a **WCS Element**:
