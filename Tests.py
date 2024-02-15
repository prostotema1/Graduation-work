from faker_pyspark import PySparkProvider
from faker import Faker
from pyspark.sql.types import *
import pandas as pd
from tabulate import tabulate
from generator.sample_generator import sample_generator


def join_2_df(df1: pd.DataFrame,
              df2: pd.DataFrame,
              join_condition: str = "a=b",
              join_type="inner") -> pd.DataFrame:
    join_condtition_splited = list(map(lambda x: str(x.strip().split(".")[1]), join_condition.split("=")))

    result = df1.merge(df2,
                       left_on=join_condtition_splited[0],
                       right_on=join_condtition_splited[-1],
                       how=join_type)
    return result


# Для спарка есть библиотека на тест assert
# spark-testing-base(java),spark-fast-test(java)
def assert_2_df_equal(data_frame_1: pd.DataFrame, data_frame_2: pd.DataFrame) -> bool:
    res = data_frame_1.compare(data_frame_2).size
    return res == 0


fake = Faker("ru_RU")
fake.add_provider(PySparkProvider)

schema1 = StructType([
    StructField('user_id', IntegerType(), False),
    StructField('user_name', StringType(), False),
    StructField('email', StringType(), False),
    StructField('smth', StringType(), False),
    StructField('float', DoubleType(), False)
])

schema2 = StructType([
    StructField('user_id', IntegerType(), False),
    StructField('date', DateType(), False),
    StructField('smth', StringType(), False),
    StructField('has_work', BooleanType(), False)
])

schema3 = StructType([
    StructField('user_id', IntegerType(), False),
    StructField('date', DateType(), False),
    StructField('purchase', StringType(),False)
])

schemas = [schema1, schema2, schema3]
sizes = [20, 20, 30]
join_condition = ["data1.user_id = data2.user_id", "data2.date = data3.date"]
join_type = ['inner', 'inner']
correlated_keys = [15, 10]

gen = sample_generator(5, 0, 1000, min_date='987-08-12', max_date='2057-11-25')
dfs = gen.generate_samples(schemas, sizes, join_condition, join_type, correlated_keys)
for i in range(len(dfs)-1):
    print("Датафрейм №", i+1)
    print(tabulate(dfs[i],headers=dfs[i].keys(),tablefmt='psql'))
    print()

print("Итоговый датасет:")
print(tabulate(dfs[-1],headers=dfs[-1].keys(),tablefmt='psql'))