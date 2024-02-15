import datetime

import pandas as pd
from pandas import DataFrame
from pyspark.sql.types import StructType, IntegerType, DoubleType, DateType, BooleanType
from pyspark.sql.types import DataType
from faker import Faker

from generator.ConfigManager import Config_Manager


class sample_generator:
    def __init__(self,
                 min_int: int = 0,
                 max_int: int = 1,
                 min_double: float = 0.0,
                 max_double: float = 1.0,
                 min_date: str = '1979-01-01',
                 max_date: str = '2047-12-02',
                 string_format='?###???#?#?',
                 letters='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ',
                 dataset_names="",
                 safe_to_csv=True):
        self.min_int = min_int
        self.max_int = max_int
        self.min_double = min_double
        self.max_double = max_double
        date1 = list(map(lambda x: int(x), min_date.split("-")))
        date2 = list(map(lambda x: int(x), max_date.split("-")))
        self.min_date = datetime.date(date1[0], date1[1], date1[2])
        self.max_date = datetime.date(date2[0], date2[1], date2[2])
        self.common_fields = dict()
        self.common_values = dict()
        self.generators = dict()
        self.string_format = string_format
        self.letters = letters
        self.safe_to_csv = safe_to_csv  # Used for save. If true dfs will be saved in csv format, else dfs will be saved as parquet
        self.dataset_names = dataset_names

    def init_with_cfg(self, path_to_file):
        cfg_manager = Config_Manager(path_to_file)
        self.dataset_names, sizes, schemas, restrictions, join_conditions, join_type, correlated_keys = cfg_manager.give_info()
        schemas = list(schemas.values())
        self.min_int = restrictions['min_int']
        self.max_int = restrictions['max_int']
        self.min_double = restrictions['min_double']
        self.max_double = restrictions['max_double']
        self.min_date = restrictions['min_date']
        self.max_date = restrictions['max_date']
        self.string_format = restrictions['string_format']
        self.letters = restrictions['letters']
        self.safe_to_csv = restrictions['save_to_csv']
        self.generate_samples(schemas,sizes,join_conditions,join_type,correlated_keys)

    def generate_not_unique_datatype(self, data_type: DataType, field_name: str):
        if field_name not in self.generators:
            self.generators[field_name] = Faker('ru_RU')
        fake = self.generators[field_name]
        if data_type == IntegerType():
            return fake.random_int(self.min_int, self.max_int)
        elif data_type == BooleanType():
            return fake.pybool()
        elif data_type == DoubleType():
            return fake.pyfloat(min_value=self.min_double, max_value=self.max_double)
        elif data_type == DateType():
            return fake.date_between_dates(date_start=self.min_date, date_end=self.max_date)
        else:
            return fake.pystr_format(self.string_format, self.letters)

    def generate_unique_datatype(self, data_type: DataType, field_name: str):
        if field_name not in self.generators:
            self.generators[field_name] = Faker('ru_RU')
        fake = self.generators[field_name]
        if data_type == IntegerType():
            return fake.unique.random_int(self.min_int,
                                          self.max_int)  # Что случится если мы попросим инт, но свободных уже не будет
        elif data_type == BooleanType():
            return fake.pybool()
        elif data_type == DoubleType():
            return fake.unique.pyfloat(min_value=self.min_double, max_value=self.max_double)
        elif data_type == DateType():
            return fake.unique.date_between_dates(date_start=self.min_date, date_end=self.max_date)
        else:
            return fake.pystr_format(self.string_format, self.letters)

    def generate_left_sample(self,
                             df: DataFrame,
                             size: int,
                             schema: StructType
                             ):
        for i in range(size):
            data = {}
            for field in schema.fields:
                res = self.generate_unique_datatype(field.dataType, field.name)
                data[field.name] = res
            df.loc[len(df)] = data

    def generate_2_correlated_samples(self,
                                      schema1: StructType,
                                      schema2: StructType,
                                      size1: int,
                                      size2: int,
                                      count_corrrelated_keys: int = 0) -> tuple[DataFrame, DataFrame]:
        df1 = pd.DataFrame(columns=schema1.fieldNames())
        df2 = pd.DataFrame(columns=schema2.fieldNames())
        for i in schema1.fields:
            if schema2.fieldNames().__contains__(i.name):
                self.common_fields[i.name] = i.dataType

        for i in range(count_corrrelated_keys):
            data1 = {}
            data2 = {}
            for field, field_type in self.common_fields.items():
                res = self.generate_unique_datatype(field_type, field)
                data1[field] = res
                data2[field] = res
                if field not in self.common_values.keys():
                    self.common_values[field] = set()
                self.common_values[field].add(res)

            for j in schema1.fields:
                if j.name not in self.common_fields.keys():
                    res = self.generate_unique_datatype(j.dataType, j.name)
                    data1[j.name] = res

            for j in schema2.fields:
                if j.name not in self.common_fields.keys():
                    res = self.generate_unique_datatype(j.dataType, j.name)
                    data2[j.name] = res

            df1.loc[len(df1)] = data1
            df2.loc[len(df2)] = data2

        self.generate_left_sample(df1, size1 - count_corrrelated_keys, schema1)
        self.generate_left_sample(df2, size2 - count_corrrelated_keys, schema2)
        return df1, df2

    def validate_conditions(self, correlated_keys: list[int],
                            schemas: list[StructType],
                            join_conditioins: list[str],
                            join_type: list[str]) -> bool:

        if len(schemas) == len(join_type) + 1 and len(schemas) == len(join_conditioins) + 1 and len(schemas) == len(
                correlated_keys) + 1:
            for i in range(len(correlated_keys) - 1):
                if correlated_keys[i] < correlated_keys[i + 1]:
                    return False
            return True
        return False

    def join_2_datasets(self,
                        df1: DataFrame,
                        df2: DataFrame,
                        join_condition: str = "a=b",
                        join_type: str = "inner"):
        join_condition_split = list(map(lambda x: str(x.strip().split(".")[1]), join_condition.split("=")))

        result = df1.merge(df2,
                           left_on=join_condition_split[0],
                           right_on=join_condition_split[-1],
                           how=join_type)
        return result

    def generate_sample_with_joined_sample(self,
                                           df1: DataFrame,
                                           schema: StructType,
                                           size: int,
                                           correlated_keys: int) -> DataFrame:
        if correlated_keys > len(df1):
            raise Exception("Correlated_keys is greater than length of joined dataframe")

        current_field = dict()
        df = pd.DataFrame(columns=schema.fieldNames())
        for i in schema.fields:
            if i.name in df1.keys().values:
                current_field[i.name] = i.dataType

        for i in range(correlated_keys):
            data = {}
            for j in schema.fields:
                if j.name in current_field.keys():
                    data[j.name] = df1[j.name][i]
                else:
                    res = self.generate_unique_datatype(j.dataType, j.name)
                    data[j.name] = res
            df.loc[len(df)] = data
        self.generate_left_sample(df, size - correlated_keys, schema)
        return df

    def save_dfs(self, dfs):
        for i in range(len(dfs) - 1):
            if self.safe_to_csv:
                if self.dataset_names != "":
                    dfs[i].to_csv(f"data/{self.dataset_names[i]}", index=False)
                else:
                    dfs[i].to_csv(f"data/Dataset№{i + 1}", index=False)
            else:
                if self.dataset_names != "":
                    dfs[i].to_parquet(f"data/{self.dataset_names[i]}", index=False)
                else:
                    dfs[i].to_parquet(f"data/Dataset№{i + 1}", index=False)
        dfs[-1].to_csv("data/Result", index=False) if self.safe_to_csv else dfs[-1].to_parquet("data/Result", index=False)


    def get_type(self,string):
        if string == IntegerType():
            return "Int64"
        elif string == DoubleType():
            return "float64"
        elif string == BooleanType():
            return "bool"
        else:
            return "object"
    def generate_samples(self,
                         schemas: list[StructType],
                         sizes: list[int],
                         join_conditions: list[str],
                         join_type: list[str],
                         correlated_keys: list[int]) -> list[DataFrame]:
        if not self.validate_conditions(correlated_keys, schemas, join_conditions, join_type):
            raise Exception("Check your conditions")
        dfs = []
        df1 = None
        for i in range(len(schemas) - 1):
            if i == 0:
                df1, df2 = self.generate_2_correlated_samples(schemas[i], schemas[i + 1], sizes[i], sizes[i + 1],
                                                              correlated_keys[i])
                dfs.append(df1)
                dfs.append(df2)
                df = self.join_2_datasets(df1, df2, join_condition=join_conditions[i], join_type=join_type[i])
                for j in df1.columns.values:
                    typer = self.get_type(schemas[0][j].dataType)
                    df[j] = df[j].astype(typer)
                for j in df2.columns.values:
                    typer = self.get_type(schemas[1][j].dataType)
                    df[j] = df[j].astype(typer)
                df1 = df
            else:
                df = self.generate_sample_with_joined_sample(df1, schemas[i + 1], sizes[i + 1], correlated_keys[i])
                dfs.append(df)
                df1 = self.join_2_datasets(df1, df, join_condition=join_conditions[i], join_type=join_type[i])
                for j in df.columns.values:
                    typer = self.get_type(schemas[i+1][j].dataType)
                    df1[j] = df1[j].astype(typer)
        dfs.append(df1)
        self.save_dfs(dfs)
        return dfs
