from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

class CrashAnalysis:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def load_data(self):
        charges_df = self.spark.read.csv(self.config['input_filepath']['charges'], header=True)
        persons_df = self.spark.read.csv(self.config['input_filepath']['primary_person'], header=True)
        damages_df = self.spark.read.csv(self.config['input_filepath']['damages'], header=True)
        units_df = self.spark.read.csv(self.config['input_filepath']['units'], header=True)
        endores_df = self.spark.read.csv(self.config['input_filepath']['endorse'], header=True)
        restrict_df = self.spark.read.csv(self.config['input_filepath']['restrict'], header=True)

        return charges_df, persons_df, damages_df, units_df, endores_df, restrict_df

    def question_1(self, persons_df: DataFrame) -> DataFrame:
        male_killed_df = persons_df.where(
                    (col('PRSN_INJRY_SEV_ID') == "KILLED") &
                    (col('PRSN_GNDR_ID') == "MALE")
        ).groupBy('CRASH_ID').agg(count('PRSN_GNDR_ID').alias('Male_Killed_CNT')).where(col('Male_Killed_CNT') > 2)
        
        num_crashes_males_killed = male_killed_df.select('CRASH_ID').count()

        no_crash_males_killed = self.spark.createDataFrame([num_crashes_males_killed], IntegerType()).toDF("Num_Of_Crashes_Males_Killed_GT2")
        
        return no_crash_males_killed.coalesce(1).write.mode('overwrite').csv(self.config['output_path']['question_1'], header=True)


    def question_2(self, units_df: DataFrame) -> DataFrame:
        two_wheeler_df = units_df.select('CRASH_ID', 'VEH_BODY_STYL_ID').where(
            col('VEH_BODY_STYL_ID').isin(["MOTORCYCLE", "POLICE MOTORCYCLE"])
        )
        num_two_wheelers = two_wheeler_df.select('VEH_BODY_STYL_ID').count()

        two_wheeler_crash = self.spark.createDataFrame([num_two_wheelers], IntegerType()).toDF("Num_Of_Two_Vehicles")
        
        return two_wheeler_crash.coalesce(1).write.mode('overwrite').csv(self.config['output_path']['question_2'], header=True)

    def question_3(self, persons_df: DataFrame, units_df: DataFrame ) -> DataFrame:
        ques_3 = persons_df.select('CRASH_ID', 'PRSN_TYPE_ID', 'PRSN_AIRBAG_ID').where(
                                    (col('PRSN_AIRBAG_ID')=="NOT DEPLOYED") & 
                                    (col('PRSN_TYPE_ID')=="DRIVER")
        ).join(units_df.select('CRASH_ID','UNIT_DESC_ID','DEATH_CNT', 'VEH_MAKE_ID'
        ).where( (col('UNIT_DESC_ID')=="MOTOR VEHICLE") & (col('DEATH_CNT')==1)), on='CRASH_ID', how='inner')
        
        ques_3_fin = ques_3.groupBy('VEH_MAKE_ID').agg(count('CRASH_ID').alias('cnt')).sort(col('cnt').desc()).select('VEH_MAKE_ID').limit(5)
        
        return ques_3_fin.coalesce(1).write.mode('overwrite').csv(self.config['output_path']['question_3'], header=True)

    def question_4(self, persons_df: DataFrame, units_df: DataFrame) -> DataFrame:
        ques_4 = persons_df.select('CRASH_ID','DRVR_LIC_TYPE_ID').where( 
                                                col('DRVR_LIC_TYPE_ID').isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])
        ).join(units_df.select('CRASH_ID','VEH_HNR_FL','VIN').where(col('VEH_HNR_FL')=="Y"), on='CRASH_ID', how='inner')
        
        ques_4_fin = ques_4.select('VIN').distinct().count()
        
        no_of_veh_hnr = self.spark.createDataFrame([ques_4_fin], IntegerType()).toDF("Num_Of_Vehicles_Driver_Hit_Run")
        
        return no_of_veh_hnr.coalesce(1).write.mode('overwrite').csv(self.config['output_path']['question_4'], header=True)

    def question_5(self, persons_df: DataFrame) -> DataFrame:
        ques_5 = persons_df.select('DRVR_LIC_STATE_ID','CRASH_ID').where( (col('PRSN_GNDR_ID')!="FEMALE") & 
                                                                        (~(col('DRVR_LIC_STATE_ID').isin(["NA", "Unknown", "Other"])))
        ).groupBy('DRVR_LIC_STATE_ID').agg(count('CRASH_ID').alias('cnt')).sort(col('cnt').desc())
        
        ques_5_fin = ques_5.select('DRVR_LIC_STATE_ID').limit(1).withColumnRenamed('DRVR_LIC_STATE_ID','Highest_No_Of_Accidents_Excl_Females')
        
        return ques_5_fin.coalesce(1).write.mode('overwrite').csv(self.config['output_path']['question_5'], header=True)

    def question_6(self, units_df: DataFrame) -> DataFrame:
        ques_6 = units_df.where(~col('VEH_MAKE_ID').isin(["NA", "UNKNOWN"])).select('TOT_INJRY_CNT', 'VEH_MAKE_ID'
        ).groupBy('VEH_MAKE_ID').agg(sum('TOT_INJRY_CNT').cast('int').alias('cnt')).sort(col('cnt').desc())
        
        ques_6_fin = ques_6.limit(5).subtract(ques_6.limit(2)).select('VEH_MAKE_ID')
        
        return ques_6_fin.coalesce(1).write.mode('overwrite').csv(self.config['output_path']['question_6'], header=True)

    def question_7(self, persons_df: DataFrame, units_df: DataFrame) -> DataFrame:
        ques_7 = persons_df.select('CRASH_ID','PRSN_ETHNICITY_ID').where(~col('PRSN_ETHNICITY_ID').isin(["NA", "UNKNOWN"])
        ).join(units_df.select('CRASH_ID','VEH_BODY_STYL_ID'), on='CRASH_ID', how='inner'
        ).where(~col('VEH_BODY_STYL_ID').isin(["NA", "UNKNOWN", "NOT REPORTED","OTHER  (EXPLAIN IN NARRATIVE)"]))
        
        ques_7_fin = ques_7.groupBy('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID').agg(count('PRSN_ETHNICITY_ID').alias('ethnic_cnt'))

        w = Window.partitionBy('VEH_BODY_STYL_ID')
        
        vehl_body_ethnic = ques_7_fin.withColumn('rank', dense_rank().over(w.orderBy(ques_7_fin['ethnic_cnt'].desc()))).where(col('rank')==1).select('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID')
        
        return vehl_body_ethnic.coalesce(1).write.mode('overwrite').csv(self.config['output_path']['question_7'], header=True)

    def question_8(self, persons_df: DataFrame, units_df: DataFrame) -> DataFrame:
        ques_8 = units_df.join(persons_df.where(col("DRVR_ZIP")!="UNKNOWN"), on='CRASH_ID', how='inner'
            ).where( (col("CONTRIB_FACTR_1_ID").contains("ALCOHOL")) | 
                     (col("CONTRIB_FACTR_2_ID").contains("ALCOHOL") | 
                     (col("CONTRIB_FACTR_P1_ID").contains("ALCOHOL"))
                     )
        ).groupby("DRVR_ZIP").agg(count('CRASH_ID').alias('count')).orderBy(col("count").desc()).select('DRVR_ZIP').limit(5)
        
        return ques_8.coalesce(1).write.mode('overwrite').csv(self.config['output_path']['question_8'], header=True)

    def question_9(self, units_df: DataFrame, damages_df: DataFrame) -> DataFrame:
        ques_9 = damages_df.join(units_df, on="CRASH_ID", how='inner').where( 
                                        ((col('VEH_DMAG_SCL_1_ID') > "DAMAGED 4") & 
                                         (~col('VEH_DMAG_SCL_1_ID').isin(["NA", "NO DAMAGE", "INVALID VALUE"]))) | 
                                        ((col('VEH_DMAG_SCL_2_ID') > "DAMAGED 4") & 
                                         (~col('VEH_DMAG_SCL_2_ID').isin(["NA", "NO DAMAGE", "INVALID VALUE"])) )).where( 
                                        (col('DAMAGED_PROPERTY').isin(["NONE",'NONE1'])) & 
                                        (col('FIN_RESP_TYPE_ID').isin(["LIABILITY INSURANCE POLICY","PROOF OF LIABILITY INSURANCE"])) )

        ques_9_fin = ques_9.select('CRASH_ID').distinct().count()
        
        cnt_crash_no_damage = self.spark.createDataFrame([ques_9_fin], IntegerType()).toDF("Num_Of_Crash_No_Damage_Car_Insurance")
        
        return cnt_crash_no_damage.coalesce(1).write.mode('overwrite').csv(self.config['output_path']['question_9'], header=True)

    def question_10(self, units_df: DataFrame, charges_df: DataFrame, persons_df: DataFrame) -> DataFrame:
        drivers_speed_off = charges_df.where(col('CHARGE').contains('SPEED')
                    ).join(persons_df.where(col('DRVR_LIC_TYPE_ID').isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])), on='CRASH_ID', how='inner')

        top_25_state = units_df.where( (col('VEH_BODY_STYL_ID').isin(["PASSENGER CAR, 4-DOOR","PASSENGER CAR, 2-DOOR"])) & 
                                       (~col('VEH_LIC_STATE_ID').isin(["NA", "98"]))
        ).groupBy("VEH_LIC_STATE_ID").agg(count('CRASH_ID').alias('count')).orderBy(col("count").desc()).limit(25).select('VEH_LIC_STATE_ID')
        
        top_25_state_fin = [row['VEH_LIC_STATE_ID'] for row in top_25_state.collect()]

        top_10_vehicle_color = units_df.where(~col('VEH_COLOR_ID').isin(["NA","98","99"])).groupBy("VEH_COLOR_ID"
                ).agg(count('CRASH_ID').alias('count')).orderBy(col("count").desc()).limit(10).select('VEH_COLOR_ID')

        top_10_vehicle_color_fin = [row['VEH_COLOR_ID'] for row in top_10_vehicle_color.collect()]

        ques_10 = drivers_speed_off.join(units_df.where( (col('VEH_COLOR_ID').isin(top_10_vehicle_color_fin)) & 
                                                         (~(col('VEH_LIC_STATE_ID').isin(top_25_state_fin))) 
        ), on='CRASH_ID', how='inner').groupBy("VEH_MAKE_ID").agg(count('CRASH_ID').alias('cnt')).orderBy(col("cnt").desc()).select('VEH_MAKE_ID').limit(5)
        
        return ques_10.coalesce(1).write.mode('overwrite').csv(self.config['output_path']['question_10'], header=True)



