import sys

import pyspark


class SparkApplicationExample:

    def __init__(self, pathsToDfs, saveResultTo):
        self.spark = pyspark.sql.SparkSession.builder.master("local").appName("Spark_Application").getOrCreate()
        self.dfs = []
        for i in pathsToDfs:
            self.dfs.append(self.spark.read.csv(i, sep=",", header=True, inferSchema=True))
        self.join_operation(["user_id", "product_id"], ["left", "right"], saveResultTo)

    def join_operation(self, operations: list[str], types, saveResTo="data/"):
        """
            We suspect that this program only operate with inner
            left,right,outer joins and operations will be input in right order
            and with just keyword(column_name) for join
            """
        result = []
        for i in range(len(operations)):
            if i == 0:
                result = self.dfs[i].join(self.dfs[i + 1], operations[i], types[i])
            else:
                result = result.join(self.dfs[i + 1], operations[i], types[i])
        result.toPandas().to_csv(f"{saveResTo}/SparkResult", index=False)


if __name__ == '__main__':
    path_to_dfs = sys.argv[1].split(",")
    save_to = sys.argv[2]
    SparkApplicationExample(path_to_dfs, save_to)
