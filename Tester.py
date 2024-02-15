import subprocess
import sys
from chispa.dataframe_comparer import assert_df_equality
import pyspark

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
    path_to_cfg = sys.argv[1]
    path_to_dfs = sys.argv[2]
    path_to_spark_app = sys.argv[3]
    path_to_result = sys.argv[4]
    run_generator_with_cfg(path_to_cfg)
    run_spark_application(path_to_spark_app, path_to_dfs,path_to_result)
    tttester(path_to_result+"/SparkResult")

# Спарк приложение должно принимать пути к файлам-исходникам и знать куда сохранять результат
# Тестер запускает спарк приложение, для этого ему нужно знать имя этой программы и какие параметры нужно передать, место где хранится ожидаемый и фактический результаты.
# Причем место где хранится ожидаемый результат надо также передавать


#Именованные аргументы в консоли, написать инструкцию по запуску, выложить на гитхаб и отправить ссылочку