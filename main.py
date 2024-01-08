from pyspark.sql import SparkSession
from Data_Processor.Data_Processor_Module import CrashAnalysis
import yaml

if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName('BCG').getOrCreate()

    # Load configuration from YAML file
    with open("config.yaml", 'r') as s:
        config = yaml.safe_load(s)

    # Create an instance of CrashAnalysis
    car_crash = CrashAnalysis(spark, config)

    # Load data
    charges_df, persons_df, damages_df, units_df, endores_df, restrict_df = car_crash.load_data()

    # question 1
    car_crash.question_1(persons_df)

    # question 2
    car_crash.question_2(units_df)

    # question 3
    car_crash.question_3(persons_df, units_df)

    # question 4
    car_crash.question_4(persons_df, units_df)

    # question 5
    car_crash.question_5(persons_df)

    # question 6
    car_crash.question_6(units_df)

    # question 7
    car_crash.question_7(persons_df, units_df)

    # question 8
    car_crash.question_8(persons_df, units_df)

    # question 9
    car_crash.question_9(units_df, damages_df)

    # question 10
    car_crash.question_10(units_df, charges_df, persons_df)

    # Stop the Spark session
    spark.stop()