from pyspark.sql import SparkSession
from data_processor import DataProcessor


def create_spark_session():
    spark = SparkSession.builder.appName("Legacy Healthcare").getOrCreate()
    return spark


def main():
    spark = create_spark_session()
    data_processor = DataProcessor(spark)
    df = data_processor.read_data("data/legacy_healthcare_data.csv")
    print("Legacy Healthcare Dataset")
    df.printSchema()

    print("DimPatient:")
    DimPatient = data_processor.generate_dim_patient(df)
    DimPatient.printSchema()
    DimPatient.show()
    data_processor.save_to_csv(DimPatient, "data/output", "DimPatient.csv")

    print("DimInsurance:")
    DimInsurance = data_processor.generate_dim_insurance(df)
    DimInsurance.printSchema()
    DimInsurance.show()
    data_processor.save_to_csv(DimInsurance, "data/output", "DimInsurance.csv")

    print("DimBilling:")
    DimBilling = data_processor.generate_dim_billing(df)
    DimBilling.printSchema()
    DimBilling.show()
    data_processor.save_to_csv(DimBilling, "data/output", "DimBilling.csv")

    print("DimProvider:")
    DimProvider = data_processor.generate_dim_provider(df)
    DimProvider.printSchema()
    DimProvider.show()
    data_processor.save_to_csv(DimProvider, "data/output", "DimProvider.csv")

    print("DimLocation:")
    DimLocation = data_processor.generate_dim_location(df)
    DimLocation.printSchema()
    DimLocation.show()
    data_processor.save_to_csv(DimLocation, "data/output", "DimLocation.csv")

    print("DimPrimaryDiagnosis:")
    DimPrimaryDiagnosis = data_processor.generate_dim_primary_diagnosis(df)
    DimPrimaryDiagnosis.printSchema()
    DimPrimaryDiagnosis.show()
    data_processor.save_to_csv(
        DimPrimaryDiagnosis, "data/output", "DimPrimaryDiagnosis.csv"
    )

    print("DimSecondaryDiagnosis:")
    DimSecondaryDiagnosis = data_processor.generate_dim_secondary_diagnosis(df)
    DimSecondaryDiagnosis.printSchema()
    DimSecondaryDiagnosis.show()
    data_processor.save_to_csv(
        DimSecondaryDiagnosis, "data/output", "DimSecondaryDiagnosis.csv"
    )

    print("DimTreatment:")
    DimTreatment = data_processor.generate_dim_treatment(df)
    DimTreatment.printSchema()
    DimTreatment.show()
    data_processor.save_to_csv(DimTreatment, "data/output", "DimTreatment.csv")

    print("DimPrescription:")
    DimPrescription = data_processor.generate_dim_prescription(df)
    DimPrescription.printSchema()
    DimPrescription.show()
    data_processor.save_to_csv(DimPrescription, "data/output", "DimPrescription.csv")

    print("DimLabOrder:")
    DimLabOrder = data_processor.generate_dim_lab_order(df)
    DimLabOrder.printSchema()
    DimLabOrder.show()
    data_processor.save_to_csv(DimLabOrder, "data/output", "DimLabOrder.csv")

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
    FactVisit.printSchema()
    FactVisit.show()
    data_processor.save_to_csv(FactVisit, "data/output", "FactVisit.csv")

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
