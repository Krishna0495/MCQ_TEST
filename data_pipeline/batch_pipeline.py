from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.window import Window

class data_ingestion:
    def __init__(self,app_name="MCQ_Test_Ingestion", master="local[*]", gcs_bucket="gs://input_bucket",error_folder="gs://error_bucket"):
        # Initialize Spark Session with GCS connector
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars", "path-to-gcs-connector/gcs-connector-hadoop3-latest.jar") \
            .getOrCreate()

        # GCS Bucket where parquet files are stored
        self.gcs_bucket = gcs_bucket
        current_date = datetime.now().date()
        current_date = str(current_date).replace("-",'')
        self.error_folder = f"{error_folder}/{current_date}"
        
    def read_parquet(self, file_path):
        """Read parquet file from GCS into Spark DataFrame"""
        full_path = f"{self.gcs_bucket}/{file_path}"
        df = self.spark.read.parquet(full_path)
        return df
    
    def write_errors(self, df, table_name, reason):
        """Write error records to a separate folder with a reason"""
        if df.count()>0:
            error_path = f"{self.error_folder}/{table_name}_errors.parquet"
            df = df.withColumn("error_reason", F.lit(reason))
            df.write.mode("append").parquet(error_path)
            print(f"Error data written to {error_path} for table {table_name} due to {reason}")

    def foreign_key_check(self, child_df, parent_df, join_columns, child_table, parent_table):
        """Check for foreign key constraint violations"""
        # Left anti-join to find rows in child_df that don't have matching rows in parent_df
        fk_violations_df = child_df.join(parent_df, join_columns, "leftanti")
        
        # if fk_violations_df.count() > 0:
            # Log the erroneous records with a reason
        self.write_errors(fk_violations_df, child_table, f"Foreign key constraint violation with {parent_table} in {str(join_columns)}")
            
        # Return the valid rows (i.e., rows that pass the FK check)
        valid_rows_df = child_df.join(parent_df, join_columns, "inner")
        return valid_rows_df
    
    # def get_max_primary_key(self, delta_table_path, pk_column):
    #     """Fetch the maximum primary key from the Delta table"""
    #     try:
    #         # Load existing data from the Delta table
    #         delta_table = self.spark.read.format("delta").load(delta_table_path)
            
    #         # Get the maximum value of the primary key
    #         max_pk_value = delta_table.agg(F.max(F.col(pk_column)).alias("max_pk")).collect()[0]["max_pk"]
    #         return max_pk_value if max_pk_value is not None else 0  # Return 0 if the table is empty
    #     except Exception as e:
    #         print(f"Table '{delta_table_path}' not found. Starting primary key at 1.")
    #         return 0  # If the table doesn't exist, start with 0

    # def add_incremental_primary_key(self, df, pk_column, start_value):
    #     """Add an incremental primary key to the DataFrame"""
    #     ##### Memory issue of spark driver in case of high data inserts #####
    #     window_spec = Window.orderBy(F.monotonically_increasing_id())
    #     df_with_pk = df.withColumn(pk_column, F.row_number().over(window_spec) + start_value)
    #     return df_with_pk
    
    def upsert_data(self, delta_table_path, new_data_df, key_columns, fk_check=None,pk_column=None):
        """Perform upsert (update + insert) logic for Delta Lake tables with optional foreign key check"""
        
        # # Generate incremental primary key if pk_column is provided
        # if pk_column:
        #     max_pk_value = self.get_max_primary_key(f"{self.gcs_bucket}/{delta_table_path}", pk_column)
        #     valid_data_df = self.add_incremental_primary_key(new_data_df, pk_column, max_pk_value)

        # else:
        #     valid_data_df = new_data_df

        if fk_check:
            parent_df, fk_columns,child_table_name,parent_table_name = fk_check
            valid_data_df = self.check_foreign_key(valid_data_df, parent_df, fk_columns,child_table_name,parent_table_name)
            
            # Log FK violation errors
            # if invalid_fk_df.count() > 0:
            # self.write_errors(invalid_fk_df, delta_table_path, fk_reason)


        
        # Delta Lake table path (for new or existing Delta tables)
        delta_table_path = f"{self.gcs_bucket}/{delta_table_path}"

        # Step 1: Perform the MERGE INTO operation for upsert (update + insert)    
        # Check if Delta table exists
        try:
            delta_table = DeltaTable.forPath(self.spark, delta_table_path)
            print(f"Delta table '{delta_table_path}' exists. Proceeding with upsert...")
            
            # Perform the upsert using Delta Lake MERGE INTO
            delta_table.alias("tgt") \
                .merge(
                    valid_data_df.alias("src"),
                    " AND ".join([f"tgt.{col} = src.{col}" for col in key_columns])  # Match condition for upsert
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()

            print(f"Upsert completed for Delta table '{delta_table_path}'.")
        
        except Exception as e:
            print(f"Delta table '{delta_table_path}' does not exist. creating new table and inserting data.")
            # If Delta table doesn't exist, create it and insert the data
            valid_data_df.write.format("delta").mode("overwrite").save(delta_table_path)

    
    def ingest_test_data(self,test_data_input_file):
        """Ingest and upsert Test table data into Delta Lake"""
        new_test_df = self.read_parquet(test_data_input_file)
        self.upsert_data("delta/test", new_test_df, ["test_id"])

    def ingest_student_data(self,student_data_input_file):
        """Ingest and upsert Test table data into Delta Lake"""
        new_test_df = self.read_parquet(student_data_input_file)
        self.upsert_data("delta/student", new_test_df, ["student_id"])

    def ingest_question_data(self,question_data_input_file):
        """Ingest and upsert Question table data with FK check on Test table"""
        new_question_df = self.read_parquet(question_data_input_file)
        test_df = self.spark.read.format("delta").load(f"{self.gcs_bucket}/delta/test")
        self.upsert_data("delta/question", new_question_df, ["question_id"], fk_check=(test_df, ["test_id"], "question","test"))

    def ingest_choice_data(self,choice_data_input_file):
        """Ingest and upsert Choice table data with FK check on Question table"""
        new_choice_df = self.read_parquet(choice_data_input_file)
        question_df = self.spark.read.format("delta").load(f"{self.gcs_bucket}/delta/question")
        self.upsert_data("delta/choice", new_choice_df, ["choice_id"], fk_check=(question_df, ["question_id"], "choice","question"))

    def ingest_answer_data(self,answer_data_input_file):
        """Ingest and upsert Answer table data with FK check on Question and Choice tables"""
        new_answer_df = self.read_parquet(answer_data_input_file)

        fk_tables_dict = {
            "test":["test_id"],
            "question":["question_id"],
            "choice":["choice_id"],
            "student":["student_id"]
                          }

        for tables,columns in fk_tables_dict.items():

            parent_df = self.spark.read.format("delta").load(f"{self.gcs_bucket}/delta/{tables}")

            valid_answer_df = self.foreign_key_check(new_answer_df, parent_df, columns,"answer",tables )
            

        self.upsert_data("delta/answer", valid_answer_df, ["answer_id"])

    
    def ingest_test_assignment_data(self,assignment_data_input_file):
        """Ingest and upsert Test Assignment table data with FK check on Test and Student tables"""

        new_test_submission_df = self.read_parquet(assignment_data_input_file)
        fk_tables_dict = {
            "test":["test_id"],
            "student":["student_id"]
                          }

        for tables,columns in fk_tables_dict.items():

            parent_df = self.spark.read.format("delta").load(f"{self.gcs_bucket}/delta/{tables}")

            valid_assignment_df = self.foreign_key_check(new_test_submission_df, parent_df, columns,"answer",tables )

        self.upsert_data("delta/test_assignment", valid_assignment_df, ["assignment_id"])


    # def ingest_session_data(self,assignment_data_input_file):
    #     """Ingest and upsert Session log table data with FK check on Test and Student tables"""

    #     new_test_submission_df = self.read_parquet(assignment_data_input_file)
    #     fk_tables_dict = {
    #         "test":["test_id"],
    #         "student":["student_id"]
    #                       }

    #     for tables,columns in fk_tables_dict.items():

    #         parent_df = self.spark.read.format("delta").load(f"{self.gcs_bucket}/delta/{tables}")

    #         valid_assignment_df = self.foreign_key_check(new_test_submission_df, parent_df, columns,"answer",tables )

    #     self.upsert_data("delta/session_log", valid_assignment_df, ["assignment_id"])


if __name__ == "__main__":
    # Initialize the data ingestion object with GCS bucket name and error folder
    data_ingestion = data_ingestion(gcs_bucket="gs://input_bucket", error_folder="gs://error_bucket/errors")

    # Ingest data into the respective Delta tables using upsert and FK check logic
    data_ingestion.ingest_test_data(test_data_input_file="test/")
    data_ingestion.ingest_student_data(student_data_input_file="student/")
    data_ingestion.ingest_question_data(question_data_input_file="question/")
    data_ingestion.ingest_choice_data(choice_data_input_file="choice/")
    data_ingestion.ingest_answer_data(answer_data_input_file="answer/")
    data_ingestion.ingest_test_assignment_data(assignment_data_input_file="assignment/")

    # Stop the Spark session
    data_ingestion.stop_spark()