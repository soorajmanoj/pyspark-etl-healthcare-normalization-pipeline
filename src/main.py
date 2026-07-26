import argparse

from pyspark.sql import SparkSession
from data_processor import DataProcessor


def create_spark_session():
    spark = SparkSession.builder.appName("Legacy Healthcare").getOrCreate()
    return spark


def parse_args():
    parser = argparse.ArgumentParser(description="Healthcare ETL pipeline")
    parser.add_argument(
        "--output-format",
        choices=["csv", "parquet", "both"],
        default="csv",
        help="Output format for dimension/fact tables (default: csv)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = create_spark_session()
    data_processor = DataProcessor(spark)
    df = data_processor.read_data("data/legacy_healthcare_data.csv")
    print("Legacy Healthcare Dataset")
    df.printSchema()

    def save_table(table_df, name):
        """Writes a table as CSV, Parquet, or both, per --output-format."""
        table_df.printSchema()
        table_df.show()
        if args.output_format in ("csv", "both"):
            data_processor.save_to_csv(table_df, "data/output", f"{name}.csv")
        if args.output_format in ("parquet", "both"):
            data_processor.save_to_parquet(table_df, "data/output_parquet", name)

    print("DimPatient:")
    DimPatient = data_processor.generate_dim_patient(df)
    save_table(DimPatient, "DimPatient")

    print("DimInsurance:")
    DimInsurance = data_processor.generate_dim_insurance(df)
    save_table(DimInsurance, "DimInsurance")

    print("DimBilling:")
    DimBilling = data_processor.generate_dim_billing(df)
    save_table(DimBilling, "DimBilling")

    print("DimProvider:")
    DimProvider = data_processor.generate_dim_provider(df)
    save_table(DimProvider, "DimProvider")

    print("DimLocation:")
    DimLocation = data_processor.generate_dim_location(df)
    save_table(DimLocation, "DimLocation")

    print("DimPrimaryDiagnosis:")
    DimPrimaryDiagnosis = data_processor.generate_dim_primary_diagnosis(df)
    save_table(DimPrimaryDiagnosis, "DimPrimaryDiagnosis")

    print("DimSecondaryDiagnosis:")
    DimSecondaryDiagnosis = data_processor.generate_dim_secondary_diagnosis(df)
    save_table(DimSecondaryDiagnosis, "DimSecondaryDiagnosis")

    print("DimTreatment:")
    DimTreatment = data_processor.generate_dim_treatment(df)
    save_table(DimTreatment, "DimTreatment")

    print("DimPrescription:")
    DimPrescription = data_processor.generate_dim_prescription(df)
    save_table(DimPrescription, "DimPrescription")

    print("DimLabOrder:")
    DimLabOrder = data_processor.generate_dim_lab_order(df)
    save_table(DimLabOrder, "DimLabOrder")

    print("FactVisit:")
    FactVisit = data_processor.generate_fact_visit(
        df,
        DimProvider,
        DimLocation,
        DimPrimaryDiagnosis,
        DimSecondaryDiagnosis,
        DimTreatment,
        DimPrescription,
        DimLabOrder,
    )
    save_table(FactVisit, "FactVisit")

    # Foreign Key Integrity Checks
    data_processor.foreign_key_check(
        FactVisit, DimPatient, "patient_id", "patient_id", "DimPatient"
    )
    data_processor.foreign_key_check(
        FactVisit, DimInsurance, "insurance_id", "insurance_id", "DimInsurance"
    )
    data_processor.foreign_key_check(
        FactVisit, DimBilling, "billing_id", "billing_id", "DimBilling"
    )
    data_processor.foreign_key_check(
        FactVisit, DimProvider, "provider_id", "provider_id", "DimProvider"
    )
    data_processor.foreign_key_check(
        FactVisit, DimLocation, "location_id", "location_id", "DimLocation"
    )
    data_processor.foreign_key_check(
        FactVisit, DimTreatment, "treatment_id", "treatment_id", "DimTreatment"
    )
    data_processor.foreign_key_check(
        FactVisit,
        DimPrimaryDiagnosis,
        "primary_diagnosis_id",
        "primary_diagnosis_id",
        "DimPrimaryDiagnosis",
    )

    # The ones with expected NULLs:
    data_processor.foreign_key_check(
        FactVisit,
        DimSecondaryDiagnosis,
        "secondary_diagnosis_id",
        "secondary_diagnosis_id",
        "DimSecondaryDiagnosis",
        allow_null=True,
    )
    data_processor.foreign_key_check(
        FactVisit,
        DimPrescription,
        "prescription_id",
        "prescription_id",
        "DimPrescription",
        allow_null=True,
    )
    data_processor.foreign_key_check(
        FactVisit,
        DimLabOrder,
        "lab_order_id",
        "lab_order_id",
        "DimLabOrder",
        allow_null=True,
    )


if __name__ == "__main__":
    main()
