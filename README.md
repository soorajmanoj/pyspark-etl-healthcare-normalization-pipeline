![Python](https://img.shields.io/badge/Python-3.x-blue.svg)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?logo=apachespark&logoColor=white)
![Tableau](https://img.shields.io/badge/Tableau-E97627?logo=tableau&logoColor=white)

# Healthcare Data Normalization with PySpark

## Overview

This personal project showcases how legacy healthcare appointment data can be normalized into a structured snowflake schema using PySpark. The aim is to create an efficient and scalable ETL pipeline that prepares data for downstream analysis and visualization.

**Note:** The dataset used in this project is entirely synthetic and was generated for educational and demonstration purposes. It does not contain any real patient information.

---

## Objective

Transform flat healthcare records (CSV format) into well-defined dimension and fact tables. Key goals include:

- Building an end-to-end ETL pipeline in PySpark.
- Creating a normalized snowflake schema with proper foreign key relationships.
- Ensuring data integrity and analytical readiness.
- Using Tableau for dashboarding and insight generation.

---

## Features

- Fully automated ETL pipeline using PySpark
  - Normalization into 10 dimension tables and 1 fact table
- Referential integrity validation across all 10 foreign key relationships
- Support for optional healthcare data (e.g., prescriptions, lab orders)
- Tableau dashboards for analytical insights

---

## Schema (ERD)

This is a **snowflake** schema: two dimensions are normalized a level further rather than flattened directly onto the fact table. `DimInsurance` carries a `patient_id` back to `DimPatient`, and `DimBilling` carries an `insurance_id` back to `DimInsurance` — so `FactVisit → DimBilling → DimInsurance → DimPatient` is a real two-hop chain, not a flat star.

```mermaid
erDiagram
    FactVisit }o--|| DimPatient : patient_id
    FactVisit }o--|| DimInsurance : insurance_id
    FactVisit }o--|| DimBilling : billing_id
    FactVisit }o--|| DimProvider : provider_id
    FactVisit }o--|| DimLocation : location_id
    FactVisit }o--|| DimPrimaryDiagnosis : primary_diagnosis_id
    FactVisit }o--o| DimSecondaryDiagnosis : secondary_diagnosis_id
    FactVisit }o--|| DimTreatment : treatment_id
    FactVisit }o--o| DimPrescription : prescription_id
    FactVisit }o--o| DimLabOrder : lab_order_id
    DimInsurance }o--|| DimPatient : patient_id
    DimBilling }o--|| DimInsurance : insurance_id

    FactVisit {
        string visit_id PK
        string patient_id FK
        string insurance_id FK
        string billing_id FK
        int provider_id FK
        int location_id FK
        int primary_diagnosis_id FK
        int secondary_diagnosis_id FK "nullable"
        int treatment_id FK
        string prescription_id FK "nullable"
        string lab_order_id FK "nullable"
        timestamp visit_datetime
        string visit_type
    }
    DimPatient {
        string patient_id PK
        string patient_first_name
        string patient_last_name
        date patient_date_of_birth
        string patient_gender
        string patient_city
        string patient_state
        string patient_status
    }
    DimInsurance {
        string insurance_id PK
        string patient_id FK
        string insurance_payer_name
        string insurance_plan_type
    }
    DimBilling {
        string billing_id PK
        string insurance_id FK
        float billing_total_charge
        float billing_amount_paid
        string billing_payment_status
    }
    DimProvider {
        int provider_id PK
        string doctor_name
        string doctor_title
        string doctor_department
    }
    DimLocation {
        int location_id PK
        string clinic_name
        string room_number
    }
    DimPrimaryDiagnosis {
        int primary_diagnosis_id PK
        string primary_diagnosis_code
        string primary_diagnosis_desc
    }
    DimSecondaryDiagnosis {
        int secondary_diagnosis_id PK
        string secondary_diagnosis_code
        string secondary_diagnosis_desc
    }
    DimTreatment {
        int treatment_id PK
        string treatment_code
        string treatment_desc
    }
    DimPrescription {
        string prescription_id PK
        string prescription_drug_name
        string prescription_dosage
        string prescription_frequency
    }
    DimLabOrder {
        string lab_order_id PK
        string lab_test_code
        string lab_result_value
        string lab_result_units
    }
```

---

## Scale

From the included synthetic dataset (`data/legacy_healthcare_data.csv`):

| Table | Rows | Notes |
|---|---|---|
| Input (legacy flat file) | 530 visits | Single flat CSV, 45 columns |
| `FactVisit` | 530 | One row per visit |
| `DimPatient` | 530 | One row per unique patient |
| `DimInsurance` | 530 | |
| `DimBilling` | 530 | |
| `DimProvider` | 181 | Deduplicated across 530 visits |
| `DimLocation` | 496 | Deduplicated clinic/room combinations |
| `DimPrescription` | 375 | Nullable — not every visit prescribes |
| `DimLabOrder` | 313 | Nullable — not every visit orders labs |
| `DimPrimaryDiagnosis` | 10 | Deduplicated diagnosis codes |
| `DimSecondaryDiagnosis` | 10 | Nullable — not every visit has one |
| `DimTreatment` | 5 | Deduplicated treatment codes |

A single flat 45-column file normalizes down to 10 focused dimension tables (as few as 3 columns each) plus a fact table — the kind of column-count reduction that's the whole point of normalization.

---

## Before / After: One Row, Traced End to End

One real row from the legacy CSV (`visit_id = Vb89557`), traced through to every table it lands in:

**Legacy input row (abridged):**
```
visit_id: Vb89557
visit_datetime: 2024-05-13 13:33:25.824916
visit_type: Follow-up
patient_id: 2ad61d54, patient_first_name: Vanessa, patient_last_name: Wilson
insurance_id: ae80b07a, insurance_payer_name: Martin Inc, insurance_plan_type: PPO
billing_id: ffada062, billing_total_charge: 1745.17, billing_payment_status: Pending
doctor_name: Adrienne Miller DVM, doctor_title: NP, doctor_department: Set designer
clinic_name: Tucker-Chang, room_number: 220
primary_diagnosis_code: Dd29351
treatment_code: T6c9074
prescription_id: 488b09ac, prescription_drug_name: manage tonight
lab_order_id: 0341123c, lab_result_value: 20.78
```

**Splits into:**

| Table | Row |
|---|---|
| `DimPatient` | `2ad61d54, Vanessa, Wilson, 1995-07-13, F, ..., Active` |
| `DimInsurance` | `ae80b07a, 2ad61d54, Martin Inc, ..., PPO` |
| `DimBilling` | `ffada062, ae80b07a, 1745.17, 1544.38, ..., Pending` |
| `DimProvider` | `139, Adrienne Miller DVM, NP, Set designer` |
| `DimLocation` | `0, Tucker-Chang, 220` |
| `DimPrimaryDiagnosis` | `3, Dd29351, That win carry believe base nation piece.` |
| `DimTreatment` | `1, T6c9074, Begin up international despite.` |
| `DimPrescription` | `488b09ac, manage tonight, 983mg, Twice daily, 16.0` |
| `DimLabOrder` | `0341123c, Lfdb996, ..., 20.78, U/L, ...` |
| `FactVisit` | `Vb89557, 2ad61d54, ae80b07a, ffada062, 139, 0, 3, , 1, 488b09ac, 0341123c, ..., Follow-up` |

Notice `FactVisit` stores `139` and `0` for provider and location, not the doctor's name or clinic name — those got assigned a surrogate integer key the first time PySpark's `monotonically_increasing_id()` encountered that unique doctor/clinic combination, and every other visit with that same doctor or clinic reuses the same ID. That's deduplication happening structurally, not just in theory.

---

## Project Structure

```
pyspark-etl-healthcare-normalization-pipeline/
├── data/
│   ├── legacy_healthcare_data.csv         # Input dataset
│   └── output/                            # Output folder for dimension and fact CSVs
├── src/
│   ├── main.py                            # Driver script
│   └── data_processor.py                  # Data normalization and FK check logic
├── tableau/
│   ├── Legacy Healthcare.twb              # Tableau workbook
│   └── tableau_screenshots/
│           └── Dashboard.png              # Tableau dashboard image
├── requirements.txt
└── README.md
```

---

## Packages Required

```
pyspark
os
shutil
glob
```

---

## How to Run

### 1. Set up the Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Execute the ETL Pipeline

```bash
python src/main.py
```

This script will:

- Read the legacy CSV file.
- Normalize and output the following tables:
  - `DimPatient`
  - `DimInsurance`
  - `DimBilling`
  - `DimProvider`
  - `DimLocation`
  - `DimPrimaryDiagnosis`
  - `DimSecondaryDiagnosis`
  - `DimTreatment`
  - `DimPrescription`
  - `DimLabOrder`
  - `FactVisit`
- Save them as individual CSVs in the `data/output/` directory.
- Run foreign key integrity checks to validate joins.

---

## Foreign Key Integrity Checks

After generating all tables, the script automatically checks whether foreign key values in `FactVisit` exist in the relevant dimension tables — all 10 relationships, including the `DimInsurance → DimPatient` and `DimBilling → DimInsurance` links.

- Null values are permitted in:
  - `secondary_diagnosis_id`
  - `prescription_id`
  - `lab_order_id`

These represent real-world scenarios where not all visits require secondary diagnoses, prescriptions, or lab orders.

---

## Dashboard Highlights (Tableau)

Tableau was used to create the following visualizations based on the normalized dataset:

- Annual Patient Visit Distribution by Clinic (2022–2025)
- Distribution of Visit Types Across All Patients
- Clinic Visit Volume by Day and Hour
- Billing Payment Status Distribution Among Visits
- Patient Count by Gender

![Dashboard Preview](https://github.com/soorajmanoj/pyspark-etl-healthcare-normalization-pipeline/raw/main/tableau/tableau_screenshots/Dashboard.png)

---

## License

MIT — see [LICENSE](LICENSE).