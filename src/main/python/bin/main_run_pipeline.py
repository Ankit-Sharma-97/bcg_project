### Import all the necessary Modules
import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date, df_count,df_top10_rec
import sys
import logging
import logging.config
import os
from run_data_ingest import load_files

### Load the Logging Configuration File
logging.config.fileConfig(fname='../util/logging_to_file.conf')


def main():
    try:
        logging.info("main() is started ...")
        ### Get Spark Object
        spark = get_spark_object(gav.envn, gav.appName)
        # Validate Spark Object
        get_curr_date(spark)

        ### Initiate run_data_ingest Script
        # Load the Dim File
        file = os.listdir(gav.staging_dim_charges)[0]
        print(file)
        file_dir_charges = gav.staging_dim_charges + file
        print(file_dir_charges)
        if file.split('.')[1] == 'csv':
            file_format = 'csv'
            header = gav.header
            inferSchema = gav.inferSchema
        elif file.split('.')[1] == 'parquet':
            file_format = 'parquet'
            header = 'NA'
            inferSchema = 'NA'

        df_charges = load_files(spark=spark, file_dir=file_dir_charges, file_format=file_format, header=header,
                                inferSchema=inferSchema)
        df_charges.show(2)

        file = os.listdir(gav.staging_dim_damages)[0]
        print(file)
        file_dir_damages = gav.staging_dim_damages + file
        print(file_dir_damages)
        if file.split('.')[1] == 'csv':
            file_format = 'csv'
            header = gav.header
            inferSchema = gav.inferSchema
        elif file.split('.')[1] == 'parquet':
            file_format = 'parquet'
            header = 'NA'
            inferSchema = 'NA'

        df_damages = load_files(spark=spark, file_dir=file_dir_damages, file_format=file_format, header=header,
                                inferSchema=inferSchema)
        df_damages.show(2)

        file = os.listdir(gav.staging_dim_endorse)[0]
        print(file)
        file_dir_endorse= gav.staging_dim_endorse + file
        print(file_dir_endorse)
        if file.split('.')[1] == 'csv':
            file_format = 'csv'
            header = gav.header
            inferSchema = gav.inferSchema
        elif file.split('.')[1] == 'parquet':
            file_format = 'parquet'
            header = 'NA'
            inferSchema = 'NA'

        df_endorse = load_files(spark=spark, file_dir=file_dir_endorse, file_format=file_format, header=header,
                                inferSchema=inferSchema)
        df_endorse.show(2)

        file = os.listdir(gav.staging_dim_restrict)[0]
        print(file)
        file_dir_restrict = gav.staging_dim_restrict + file
        print(file_dir_restrict)
        if file.split('.')[1] == 'csv':
            file_format = 'csv'
            header = gav.header
            inferSchema = gav.inferSchema
        elif file.split('.')[1] == 'parquet':
            file_format = 'parquet'
            header = 'NA'
            inferSchema = 'NA'

        df_restrict = load_files(spark=spark, file_dir=file_dir_restrict, file_format=file_format, header=header,
                                inferSchema=inferSchema)
        df_restrict.show(2)

        #validate fact for usa_accident_pipeline

        df_count(df_charges, 'df_charges')
        df_top10_rec(df_charges, 'df_charges')

        df_count(df_damages, 'df_damages')
        df_top10_rec(df_damages, 'df_damages')

        df_count(df_restrict, 'df_restrict')
        df_top10_rec(df_restrict, 'df_restrict')

        # Load the Fact File
        file = os.listdir(gav.staging_fact_Unit)[0]
        print(file)
        file_dir_unit = gav.staging_fact_Unit + file
        print(file_dir_unit)
        if file.split('.')[1] == 'csv':
            file_format = 'csv'
            header = gav.header
            inferSchema = gav.inferSchema
        elif file.split('.')[1] == 'parquet':
            file_format = 'parquet'
            header = 'NA'
            inferSchema = 'NA'

        df_unit = load_files(spark=spark, file_dir=file_dir_unit, file_format=file_format, header=header,
                                inferSchema=inferSchema)
        df_unit.show(2)

        file = os.listdir(gav.staging_fact_person)[0]
        print(file)
        file_dir_person = gav.staging_fact_person + file
        print(file_dir_person)
        if file.split('.')[1] == 'csv':
            file_format = 'csv'
            header = gav.header
            inferSchema = gav.inferSchema
        elif file.split('.')[1] == 'parquet':
            file_format = 'parquet'
            header = 'NA'
            inferSchema = 'NA'

        df_person = load_files(spark=spark, file_dir=file_dir_person, file_format=file_format, header=header,
                                inferSchema=inferSchema)
        df_person.show(2)
        # Validate the fact data
        df_count(df_unit, 'df_unit')
        df_top10_rec(df_person, 'df_person')

        df_count(df_restrict, 'df_city')
        df_top10_rec(df_restrict, 'df_city')
        # Set up Error Handling
        # Set up Logging Configuration Mechanism

        ### Initiate run_presc_data_preprocessing Script
        # Perform data Cleaning Operations
        # Validate
        # Set up Error Handling
        # Set up Logging Configuration Mechanism

        ### Initiate run_presc_data_transform Script
        # Apply all the transfrmations Logics
        # Validate

        # Set up Logging Configuration Mechanism

        ### Initiate run_data_extraction Script
        # Validate
        # Set up Error Handling
        # Set up Logging Configuration Mechanism

        ### End of Application Part 1
        logging.info("presc_run_pipeline.py is Completed.")

    except Exception as exp:
        logging.error("Error Occurred in the main() method. Please check the Stack Trace to go to the respective module "
                      "and fix it." + str(exp), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    logging.info("run_presc_pipeline is Started ...")
    main()
