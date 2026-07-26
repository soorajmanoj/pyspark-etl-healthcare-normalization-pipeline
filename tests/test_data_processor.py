"""
Tests for DataProcessor — exercises the real business logic in
src/data_processor.py against small in-memory sample data:

- generate_dim_patient: dedup on patient_id, Active/Inactive status derivation
- generate_dim_insurance: dedup + FK column retention
- generate_dim_provider: dedup + surrogate key assignment
- foreign_key_check: pass case, mismatch case, and allow_null handling

Run from the project root: pytest
"""

from src.data_processor import DataProcessor


def make_sample_rows():
    """Two visits for two different patients — one recent (Active),
    one from 2021 (Inactive) — plus enough columns for every
    generate_dim_* method under test to run without error."""
    base = {
        "patient_address_line1": "1 Main St",
        "patient_address_line2": None,
        "patient_city": "Springfield",
        "patient_state": "IL",
        "patient_zip": "62704",
        "patient_phone": "555-0100",
        "patient_email": "vanessa@example.com",
        "insurance_policy_number": "POL1",
        "insurance_group_number": "GRP1",
        "billing_total_charge": 100.0,
        "billing_amount_paid": 80.0,
        "billing_date": "2024-05-21",
        "billing_payment_status": "Pending",
        "primary_diagnosis_code": "D001",
        "primary_diagnosis_desc": "Hypertension",
        "secondary_diagnosis_code": None,
        "secondary_diagnosis_desc": None,
        "treatment_code": "T001",
        "treatment_desc": "Medication review",
        "prescription_id": "RX1",
        "prescription_drug_name": "Lisinopril",
        "prescription_dosage": "10mg",
        "prescription_frequency": "Once daily",
        "prescription_duration_days": 30.0,
        "lab_order_id": "L1",
        "lab_test_code": "LT1",
        "lab_name": "Basic Metabolic Panel",
        "lab_result_value": 5.4,
        "lab_result_units": "mmol/L",
        "lab_result_date": "2024-05-14",
    }

    row_active = dict(
        base,
        visit_id="V001",
        visit_datetime="2024-05-13 13:33:25",
        visit_type="Follow-up",
        patient_id="P1",
        patient_first_name="Vanessa",
        patient_last_name="Wilson",
        patient_date_of_birth="1995-07-13",
        patient_gender="F",
        insurance_id="I1",
        insurance_payer_name="Acme Insurance",
        insurance_plan_type="PPO",
        billing_id="B1",
        doctor_name="Dr. Smith",
        doctor_title="MD",
        doctor_department="Cardiology",
        clinic_name="Downtown Clinic",
        room_number="101",
    )

    row_inactive = dict(
        base,
        visit_id="V002",
        visit_datetime="2021-03-02 09:00:00",
        visit_type="Routine",
        patient_id="P2",
        patient_first_name="Marcus",
        patient_last_name="Lee",
        patient_date_of_birth="1980-01-01",
        patient_gender="M",
        insurance_id="I2",
        insurance_payer_name="Beta Health",
        insurance_plan_type="HMO",
        billing_id="B2",
        doctor_name="Dr. Patel",
        doctor_title="DO",
        doctor_department="Internal Medicine",
        clinic_name="Uptown Clinic",
        room_number="202",
    )

    return [row_active, row_inactive]


def make_sample_df(spark):
    """
    Builds the sample DataFrame with an explicit schema rather than relying
    on PySpark's type inference. Several columns (patient_address_line2,
    secondary_diagnosis_code, secondary_diagnosis_desc) are None in every
    sample row by design — inference can't determine a type when a column
    is null across the whole sample, so we declare the schema instead of
    inferring it. Every column here is StringType, which is safe because
    none of the code under test (generate_dim_patient, generate_dim_insurance,
    generate_dim_provider, foreign_key_check) does numeric computation on
    the billing/lab columns — the one place a string IS compared
    (last_visit <= "2021-12-31" in generate_dim_patient) works correctly
    under plain lexicographic string comparison for zero-padded ISO-style
    timestamps.
    """
    from pyspark.sql.types import StructType, StructField, StringType

    rows = make_sample_rows()
    columns = sorted({key for row in rows for key in row.keys()})
    schema = StructType([StructField(c, StringType(), True) for c in columns])
    data = [
        tuple(None if row.get(c) is None else str(row.get(c)) for c in columns)
        for row in rows
    ]
    return spark.createDataFrame(data, schema=schema)


def test_generate_dim_patient_dedup_and_status(spark):
    df = make_sample_df(spark)
    dp = DataProcessor(spark)

    dim_patient = dp.generate_dim_patient(df)
    rows = {row["patient_id"]: row["patient_status"] for row in dim_patient.collect()}

    assert dim_patient.count() == 2
    assert rows["P1"] == "Active"    # 2024 visit -> Active
    assert rows["P2"] == "Inactive"  # 2021 visit -> Inactive


def test_generate_dim_insurance_retains_patient_fk(spark):
    df = make_sample_df(spark)
    dp = DataProcessor(spark)

    dim_insurance = dp.generate_dim_insurance(df)
    row = dim_insurance.filter(dim_insurance.insurance_id == "I1").collect()[0]

    assert dim_insurance.count() == 2
    assert row["patient_id"] == "P1"  # confirms the DimInsurance -> DimPatient FK survives


def test_generate_dim_provider_dedup_and_surrogate_key(spark):
    df = make_sample_df(spark)
    dp = DataProcessor(spark)

    dim_provider = dp.generate_dim_provider(df)
    provider_ids = [row["provider_id"] for row in dim_provider.collect()]

    assert dim_provider.count() == 2         # two distinct doctors in sample data
    assert len(set(provider_ids)) == 2       # surrogate keys are unique per provider


def test_foreign_key_check_passes_when_all_keys_match(spark, capsys):
    df = make_sample_df(spark)
    dp = DataProcessor(spark)

    dim_patient = dp.generate_dim_patient(df)
    fact_like = df.select("visit_id", "patient_id")

    dp.foreign_key_check(fact_like, dim_patient, "patient_id", "patient_id", "DimPatient", allow_null=False)
    output = capsys.readouterr().out

    assert "✅" in output
    assert "❌" not in output


def test_foreign_key_check_detects_mismatch(spark, capsys):
    df = make_sample_df(spark)
    dp = DataProcessor(spark)

    dim_patient = dp.generate_dim_patient(df)
    # Introduce a fact row referencing a patient_id that doesn't exist in the dimension
    broken_fact = spark.createDataFrame(
        [{"visit_id": "V999", "patient_id": "DOES_NOT_EXIST"}]
    )

    dp.foreign_key_check(broken_fact, dim_patient, "patient_id", "patient_id", "DimPatient", allow_null=False)
    output = capsys.readouterr().out

    assert "❌" in output
    assert "1 unmatched" in output


def test_foreign_key_check_allows_expected_nulls(spark, capsys):
    df = make_sample_df(spark)
    dp = DataProcessor(spark)

    dim_secondary = dp.generate_dim_secondary_diagnosis(df)
    # Both sample rows have a null secondary_diagnosis_code/desc, so no dim rows are generated.
    # secondary_diagnosis_id is None here too -> needs an explicit schema, same reason as
    # make_sample_df above (inference can't determine a type for an all-null column).
    from pyspark.sql.types import StructType, StructField, StringType

    fact_like = spark.createDataFrame(
        [("V001", None)],
        schema=StructType([
            StructField("visit_id", StringType(), True),
            StructField("secondary_diagnosis_id", StringType(), True),
        ]),
    )

    dp.foreign_key_check(
        fact_like, dim_secondary, "secondary_diagnosis_id", "secondary_diagnosis_id",
        "DimSecondaryDiagnosis", allow_null=True,
    )
    output = capsys.readouterr().out

    assert "❌" not in output
    assert "NULL" in output  # the "expected and skipped" note should fire