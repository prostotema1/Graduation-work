import unittest
from tabulate import tabulate

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, BooleanType, DoubleType

from generator.sample_generator import sample_generator
from generator.ConfigManager import Config_Manager

class Tests(unittest.TestCase):
    join_type = None
    join_condition = None
    correlated_keys = None
    sizes = None
    schemas = None
    gen = None
    dfs = None

    @classmethod
    def print_dfs(cls):
        for i in range(len(cls.dfs) - 1):
            print("Датасет №", i + 1)
            print(tabulate(cls.dfs[i], headers=cls.dfs[i].keys(), tablefmt='psql'))
            print()
        print('Итоговый датасет:')
        print(tabulate(cls.dfs[-1], headers=cls.dfs[-1].keys(), tablefmt='psql'))

    @classmethod
    def setUpClass(cls):
        schema1 = StructType([
            StructField('user_id', IntegerType(), False),
            StructField('name', StringType(), False),
            StructField('surname', StringType(), False),
            StructField('birthdate', DateType(), False),
        ])
        schema2 = StructType([
            StructField('user_id', IntegerType(), False),
            StructField('purchase', StringType(), False),
            StructField('date', DateType(), False),
        ])
        schema3 = StructType([
            StructField('date', DateType(), False),
            StructField('item', StringType(), False),
            StructField('is_bought', BooleanType(), False),
        ])
        schema4 = StructType([
            StructField('item', StringType(), False),
            StructField('weight', DoubleType(), False),
            StructField('description', StringType(), False)
        ])

        cls.schemas = [schema1, schema2, schema3, schema4]
        cls.sizes = [1000, 1000, 1000, 1000]  # 100,100,100,100
        cls.correlated_keys = [20, 20, 10]
        cls.join_condition = ['df1.user_id=df2.user_id', 'df2.date=df3.date', 'df3.item=df4.item']
        cls.join_type = ['left', 'right', 'inner']
        cls.gen = sample_generator(0, 5000, 0.0, 100.0)
        cls.dfs = cls.gen.generate_samples(cls.schemas, cls.sizes, cls.join_condition, cls.join_type,
                                           cls.correlated_keys)
        cls.print_dfs()

    def test_len_dfs(self):
        self.assertEqual(len(self.dfs), len(self.schemas) + 1)

    def test_len_df(self):
        for i in range(len(self.dfs) - 1):
            self.assertEqual(len(self.dfs[i]), self.sizes[i])

    def test_correlated_keys_count(self):
        df1 = None
        for i in range(len(self.join_condition)):
            if i == 0:
                df1 = self.gen.join_2_datasets(self.dfs[i], self.dfs[i + 1], self.join_condition[i], self.join_type[i],i)
            else:
                df1 = self.gen.join_2_datasets(df1, self.dfs[i + 1], self.join_condition[i], self.join_type[i],i)
            self.assertEqual(len(df1), self.correlated_keys[i])

    def test_len_result_df(self):
        self.assertEqual(len(self.dfs[-1]), min(self.correlated_keys))

# handle exception, Инпут в приложение в виде yaml или hocon подается инфа про генерацию датасетов
# В конфигурации также будет подаваться количество null элементов для каждого из столбцов для каждого датасета
