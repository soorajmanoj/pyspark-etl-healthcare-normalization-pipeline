from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    max as spark_max,
    when,
    monotonically_increasing_id,
)
from pyspark.sql.window import Window
import os
import glob
import shutil


class DataProcessor:
    def __init__(self, spark: SparkSession):
        """
        Initialize the DataProcessor with a Spark session.
        :param spark: SparkSession object
        """
        self.spark = spark

    def read_data(self, input_path):
        """
        Read CSV data into a DataFrame.
        :param input_path: Path to the CSV file
        :return: DataFrame
        """
        df = self.spark.read.csv(input_path, header=True, inferSchema=True)
        return df

    def save_to_csv(self, df: DataFrame, output_path: str, filename: str) -> None:
        """
        Save DataFrame to a single CSV file.

        :param df: DataFrame to save
        :param output_path: Base directory path
        :param filename: Name of the CSV file
        """
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)

        # Create full path for the output file
        full_path = os.path.join(output_path, filename)
        print(f"Saving to: {full_path}")  # Debugging output

        # Create a temporary directory in the correct output path
        temp_dir = os.path.join(output_path, "_temp")
        print(f"Temporary directory: {temp_dir}")  # Debugging output

        # Save to temporary directory
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

        # Find the generated part file
        csv_file = glob.glob(f"{temp_dir}/part-*.csv")[0]

        # Move and rename it to the desired output path
        shutil.move(csv_file, full_path)

        # Clean up - remove the temporary directory
        shutil.rmtree(temp_dir)

    def save_to_parquet(self, df: DataFrame, output_path: str, dirname: str) -> None:
        """
        Save DataFrame to Parquet format.

        Unlike save_to_csv, this does not coalesce to a single file first —
        multiple part files inside a Parquet "directory" is the normal,
        expected layout for a columnar warehouse format, not something to
        collapse away. Real data warehouses (Redshift Spectrum, Athena,
        Snowflake external tables, Delta Lake, etc.) read Parquet directories
        of many part files directly.

        :param df: DataFrame to save
        :param output_path: Base directory path
        :param dirname: Name of the output Parquet directory
        """
        full_path = os.path.join(output_path, dirname)
        print(f"Saving Parquet to: {full_path}")
        df.write.mode("overwrite").parquet(full_path)

    def generate_dim_patient(self, df):
        last_visit_df = df.groupBy("patient_id").agg(
            spark_max("visit_datetime").alias("last_visit")
        )
        patient_df = df.join(last_visit_df, on="patient_id", how="left")

        patient_df = patient_df.withColumn(
            "patient_status",
            when(col("last_visit") <= "2021-12-31", "Inactive").otherwise("Active"),
        )

        dim_patient = patient_df.select(
            "patient_id",
            "patient_first_name",
            "patient_last_name",
            "patient_date_of_birth",
            "patient_gender",
            "patient_address_line1",
            "patient_address_line2",
            "patient_city",
            "patient_state",
            "patient_zip",
            "patient_phone",
            "patient_email",
            "patient_status",
        ).dropDuplicates(["patient_id"])

        return dim_patient

    def generate_dim_insurance(self, df):
        insurance_df = df.select(
            "insurance_id",
            "patient_id",
            "insurance_payer_name",
            "insurance_policy_number",
            "insurance_group_number",
            "insurance_plan_type",
        ).dropDuplicates(["insurance_id"])
        return insurance_df

    def generate_dim_billing(self, df):
        biliing_df = df.select(
            "billing_id",
            "insurance_id",
            "billing_total_charge",
            "billing_amount_paid",
            "billing_date",
            "billing_payment_status",
        ).dropDuplicates(["billing_id"])
        return biliing_df

    def generate_dim_provider(self, df):
        provider_df = df.select(
            "doctor_name", "doctor_title", "doctor_department"
        ).dropDuplicates()

        provider_df = provider_df.withColumn(
            "provider_id", monotonically_increasing_id()
        )

        provider_df = provider_df.select(
            "provider_id", "doctor_name", "doctor_title", "doctor_department"
        )
        return provider_df

    def generate_dim_location(self, df):
        location_df = df.select("clinic_name", "room_number").dropDuplicates()

        location_df = location_df.withColumn(
            "location_id", monotonically_increasing_id()
        )
        location_df = location_df.select("location_id", "clinic_name", "room_number")
        return location_df

    def generate_dim_primary_diagnosis(self, df):
        prim_diag_df = df.select(
            "primary_diagnosis_code", "primary_diagnosis_desc"
        ).dropDuplicates()

        prim_diag_df = prim_diag_df.withColumn(
            "primary_diagnosis_id", monotonically_increasing_id()
        )
        prim_diag_df = prim_diag_df.select(
            "primary_diagnosis_id", "primary_diagnosis_code", "primary_diagnosis_desc"
        )
        return prim_diag_df

    def generate_dim_secondary_diagnosis(self, df):
        second_diag_df = df.select(
            "secondary_diagnosis_code", "secondary_diagnosis_desc"
        ).dropDuplicates()

        second_diag_df = second_diag_df.dropna(how="all")
        second_diag_df = second_diag_df.withColumn(
            "secondary_diagnosis_id", monotonically_increasing_id()
        )

        second_diag_df = second_diag_df.select(
            "secondary_diagnosis_id",
            "secondary_diagnosis_code",
            "secondary_diagnosis_desc",
        )
        return second_diag_df

    def generate_dim_treatment(self, df):
        treatment_df = df.select("treatment_code", "treatment_desc").dropDuplicates()

        treatment_df = treatment_df.withColumn(
            "treatment_id", monotonically_increasing_id()
        )
        treatment_df = treatment_df.select(
            "treatment_id", "treatment_code", "treatment_desc"
        )
        return treatment_df

    def generate_dim_prescription(self, df):
        prescription_df = df.select(
            "prescription_id",
            "prescription_drug_name",
            "prescription_dosage",
            "prescription_frequency",
            "prescription_duration_days",
        ).dropDuplicates(["prescription_id"])

        prescription_df = prescription_df.dropna(how="all")

        return prescription_df

    def generate_dim_lab_order(self, df):
        lab_df = df.select(
            "lab_order_id",
            "lab_test_code",
            "lab_name",
            "lab_result_value",
            "lab_result_units",
            "lab_result_date",
        ).dropDuplicates(["lab_order_id"])

        lab_df = lab_df.dropna(how="all")

        return lab_df

    def generate_fact_visit(
        self,
        df,
        DimProvider,
        DimLocation,
        DimPrimaryDiagnosis,
        DimSecondaryDiagnosis,
        DimTreatment,
        DimPrescription,
        DimLabOrder,
    ):
        """
        Generate the FactVisit table by joining the main DataFrame with dimension tables.
        :param df: Main DataFrame
        :param DimProvider: Dimension table for providers
        :param DimLocation: Dimension table for locations
        :param DimPrimaryDiagnosis: Dimension table for primary diagnosis
        :param DimSecondaryDiagnosis: Dimension table for secondary diagnosis
        :param DimTreatment: Dimension table for treatments
        :param DimPrescription: Dimension table for prescriptions
        :param DimLabOrder: Dimension table for lab orders
        :return: FactVisit DataFrame
        """
        fact_df = (
            df.join(
                DimProvider,
                on=["doctor_name", "doctor_title", "doctor_department"],
                how="left",
            )
            .join(DimLocation, on=["clinic_name", "room_number"], how="left")
            .join(
                DimPrimaryDiagnosis,
                on=["primary_diagnosis_code", "primary_diagnosis_desc"],
                how="left",
            )
            .join(
                DimSecondaryDiagnosis,
                on=["secondary_diagnosis_code", "secondary_diagnosis_desc"],
                how="left",
            )
            .join(DimTreatment, on=["treatment_code", "treatment_desc"], how="left")
        )

        fact_visit = fact_df.select(
            "visit_id",
            "patient_id",
            "insurance_id",
            "billing_id",
            "provider_id",
            "location_id",
            "primary_diagnosis_id",
            "secondary_diagnosis_id",
            "treatment_id",
            "prescription_id",
            "lab_order_id",
            "visit_datetime",
            "visit_type",
        ).dropDuplicates(["visit_id"])

        return fact_visit

    def foreign_key_check(
        self, fact_df, dim_df, fk_col, dim_key_col, dim_name, allow_null=True
    ):
        """
        Checks foreign key integrity between a fact table and a dimension table.

        Parameters:
        - fact_df: FactVisit DataFrame
        - dim_df: Dimension DataFrame
        - fk_col: Column name in FactVisit referencing the dimension
        - dim_key_col: Primary key column in the dimension table
        - dim_name: Name of the dimension (for printing)
        - allow_null: If True, nulls in the FK are considered expected and excluded from the mismatch check
        """
        # Filter out nulls in foreign key if nulls are expected
        if allow_null:
            non_null_fact = fact_df.filter(col(fk_col).isNotNull())
            null_count = fact_df.filter(col(fk_col).isNull()).count()
        else:
            non_null_fact = fact_df
            null_count = 0

        # Find mismatches
        mismatched = non_null_fact.join(
            dim_df, non_null_fact[fk_col] == dim_df[dim_key_col], how="left_anti"
        )

        mismatch_count = mismatched.count()

        # Reporting
        if mismatch_count == 0:
            print(f"✅ Foreign key integrity passed for '{dim_name}'.")
        else:
            print(
                f"❌ Foreign key mismatch detected in '{dim_name}': {mismatch_count} unmatched rows."
            )

        if allow_null and null_count > 0:
            print(
                f"🟡 Note: {null_count} NULL '{fk_col}' entries in '{dim_name}' were expected and skipped."
            )
