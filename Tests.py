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
# print("Исходные датасеты: ")
# print(tabulate(df1, headers=df1.keys(), tablefmt='psql'), "\n\n")
# print(tabulate(df2, headers=df2.keys(), tablefmt='psql'))
#
# result = join_2_df(df1, df2, join_condition="data1.user_id=data2.user_id")
# result2 = df1.merge(df2, left_on='user_id', right_on='user_id', how='inner')
# print("######################################################\nСоединенные датасеты при помощи inner-join:")
#
# print(tabulate(result, headers=result.keys(), tablefmt='psql'))
#
# print(assert_2_df_equal(result, result2))

# Последовательность генерации: генерим датасеты размеров n и m, делаем k совпадающих ключей
# \forall i!=j : x_i!=x_j ,\forall x_i\in{x_1,...,x_k} - ключ соединения в таблицах
# 0) Пользователь дает схемы,условия соединения(пока только inner) и правильный порядок соединения.
#    Исходя из этого генерируем данные по алгоритму:
# 1) Генерируем k х-ов разных
# 2) Генерируем n-k,m-k ключей таких, что любые оставшиеся не совпадали с k исходными
# 3) Генерируем произвольные данные для всех остальных столбцов
# 4) Джоиним, проверяем, делаем выводы.
# 5) Всего надо научиться пока что генерировать такие датасеты с 5 самыми популярными датасетами

# Хотим проверять конвейры с функциональной точки зрения. Отличия: там все ручками, а тут стремимся автоматизировать.


# aij = i<->j
# Датасе С приделать к результату df1.join(df2)

# Есть 2 датасета A и B, сгенерированные по условиям. Джоиним по условиям и получаем AB(\Intersect_{i=1}^{n-1} A_i). A_1 = 1 датасет, А_2 = 2 датасет, А_{1,2,3} = A_{1,2} + A_3
# Хотим построить датасет C с q коррелирующими ключами к AB с ограничением q< len(AB)
# len(С) >= q


# 1,2 генерируем как обычно, джоиним. Затем нам надо сгенерировать q коррелирующих ключей между C и AB.
