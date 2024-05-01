import subprocess
from chispa.dataframe_comparer import assert_df_equality
import pyspark
import argparse

from generator.sample_generator import sample_generator


def tttester(path_to_spark_result):
    try:
        spark = pyspark.sql.SparkSession.builder.master("local").appName("Spark Tester").getOrCreate()
        result_from_spark_application = spark.read.csv(path_to_spark_result, header=True, inferSchema=True)
        result_from_generator = spark.read.csv("data/Result", header=True, inferSchema=True)
        assert_df_equality(result_from_generator, result_from_spark_application,
                           ignore_row_order=True,
                           ignore_column_order=True)
        print("Everything works fine!")
    except Exception as e:
        print(e)
        print("Seems like spark application works not as planned")
    finally:
        spark.stop()


def run_generator_with_cfg(path_to_cfg):
    sample_generator().init_with_cfg(path_to_cfg)


def run_spark_application(programm, programm_args,path_to_res):
    try:
        subprocess.call(["python",programm,programm_args,path_to_res],shell=True)
    except subprocess.CalledProcessError as e:
        print()


if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--configuration",help="Insert path to your yaml configuration file")
    # parser.add_argument("--dfs",help="Insert paths to dfs generated from generator. By default"
    #                                  "all dataframes will be saved to /root/data/")
    # parser.add_argument("--spark",help="Insert path to your spark application")
    # parser.add_argument("--res",help="Insure the path, where the output of your spark application need to be saved")
    # args = parser.parse_args()
    path_to_cfg = "./config.yaml"
    path_to_dfs = "data/user_info,data/user_buy,data/product.csv"
    path_to_spark_app = "spark_app/spark_appp.py"
    path_to_result = "data/res"
    run_generator_with_cfg(path_to_cfg)
    run_spark_application(path_to_spark_app, path_to_dfs,path_to_result)
    tttester(path_to_result+"/SparkResult")