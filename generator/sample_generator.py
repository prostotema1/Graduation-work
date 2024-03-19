import datetime
import random

import pandas as pd
from pandas import DataFrame
from pyspark.sql.types import StructType, IntegerType, DoubleType, DateType, BooleanType, StringType
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
        self.possible_values = {}
        self.unique_restrictions = {}
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
        self.dataset_names, sizes, schemas, not_unique_restrictions, unique_restriction, possible_values, join_conditions, join_type, correlated_keys = cfg_manager.give_info()
        schemas = list(schemas.values())
        self.min_int = not_unique_restrictions['min_int']
        self.max_int = not_unique_restrictions['max_int']
        self.min_double = not_unique_restrictions['min_double']
        self.max_double = not_unique_restrictions['max_double']
        self.min_date = not_unique_restrictions['min_date']
        self.max_date = not_unique_restrictions['max_date']
        self.string_format = not_unique_restrictions['string_format']
        self.letters = not_unique_restrictions['letters']
        self.safe_to_csv = not_unique_restrictions['save_to_csv']
        self.unique_restrictions = unique_restriction
        self.possible_values = possible_values
        self.generate_samples(schemas, sizes, join_conditions, join_type, correlated_keys)

    def generate_not_unique_datatype(self, data_type: DataType, field_name: str, dataset_name: str):
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

    def generate_unique_datatype(self, data_type: DataType, field_name: str, dataset_name: str):
        if field_name not in self.generators:
            self.generators[field_name] = Faker('ru_RU')
        fake = self.generators[field_name]
        if f"{dataset_name}.{field_name}" in self.unique_restrictions:
            if data_type == IntegerType():
                min_val = self.unique_restrictions[f"{dataset_name}.{field_name}"][0]
                max_val = self.unique_restrictions[f"{dataset_name}.{field_name}"][1]
                return fake.unique.random_int(min_val, max_val)
            elif data_type == DoubleType():
                min_val = self.unique_restrictions[f"{dataset_name}.{field_name}"][0]
                max_val = self.unique_restrictions[f"{dataset_name}.{field_name}"][1]
                return fake.unique.pyfloat(min_value=min_val, max_value=max_val)
            elif data_type == DateType():
                min_date = list(
                    map(lambda x: int(x), self.unique_restrictions[f"{dataset_name}.{field_name}"][0].split("-")))
                min_date = datetime.date(min_date[0], min_date[1], min_date[2])
                max_date = list(
                    map(lambda x: int(x), self.unique_restrictions[f"{dataset_name}.{field_name}"][1].split("-")))
                max_date = datetime.date(max_date[0], max_date[1], max_date[2])
                return fake.unique.date_between_dates(date_start=min_date, date_end=max_date)
            elif data_type == StringType():
                str_format = self.unique_restrictions[f"{dataset_name}.{field_name}"][0]
                letters = self.unique_restrictions[f"{dataset_name}.{field_name}"][1]
                return fake.pystr_format(str_format, letters)

        elif f"{dataset_name}.{field_name}" in self.possible_values:
            if self.possible_values[f"{dataset_name}.{field_name}"][0] == True:
                return fake.random_choices(self.possible_values[f"{dataset_name}.{field_name}"][1], length=1)[0]
            else:
                value = fake.random_choices(self.possible_values[f"{dataset_name}.{field_name}"][1], length=1)[0]
                self.possible_values[f"{dataset_name}.{field_name}"][1].remove(value)
                return value

        if data_type == IntegerType():
            return fake.unique.random_int(self.min_int,
                                          self.max_int)
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
                             schema: StructType,
                             df_name: str
                             ):
        for i in range(size):
            data = {}
            for field in schema.fields:
                res = self.generate_unique_datatype(field.dataType, field.name, df_name)
                data[field.name] = res
            df.loc[len(df)] = data

    def generate_2_correlated_samples(self,
                                      schema1: StructType,
                                      schema2: StructType,
                                      size1: int,
                                      size2: int,
                                      name1: str,
                                      name2: str,
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
                res = self.generate_unique_datatype(field_type, field, name1)
                data1[field] = res
                data2[field] = res
                if field not in self.common_values.keys():
                    self.common_values[field] = set()
                self.common_values[field].add(res)

            for j in schema1.fields:
                if j.name not in self.common_fields.keys():
                    res = self.generate_unique_datatype(j.dataType, j.name, name1)
                    data1[j.name] = res

            for j in schema2.fields:
                if j.name not in self.common_fields.keys():
                    res = self.generate_unique_datatype(j.dataType, j.name, name2)
                    data2[j.name] = res

            df1.loc[len(df1)] = data1
            df2.loc[len(df2)] = data2
        self.generate_left_sample(df1, size1 - count_corrrelated_keys, schema1, name1)
        self.generate_left_sample(df2, size2 - count_corrrelated_keys, schema2, name2)
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
                        join_type: str = "inner",
                        i: int =0):
        join_condition_split = list(map(lambda x: str(x.strip().split(".")[1]), join_condition.split("=")))
        df1name = self.dataset_names[i] if self.dataset_names != "" else "dataset1"
        df2name = self.dataset_names[i+1] if self.dataset_names != "" else "dataset1"
        result = df1.merge(df2,
                           left_on=join_condition_split[0],
                           right_on=join_condition_split[-1],
                           how=join_type,
                           suffixes=(f'.{df1name}',f'.{df2name}'))
        return result

    def generate_sample_with_joined_sample(self,
                                           df1: DataFrame,
                                           schema: StructType,
                                           size: int,
                                           dataset_name: str,
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
                    res = self.generate_unique_datatype(j.dataType, j.name, dataset_name)
                    data[j.name] = res
            df.loc[len(df)] = data
        self.generate_left_sample(df, size - correlated_keys, schema, dataset_name)
        return df

    def save_dfs(self, dfs):
        for i in range(len(dfs) - 1):
            if self.safe_to_csv:
                if self.dataset_names != "":
                    dfs[i].to_csv(f"data/{self.dataset_names[i]}.csv", index=False)
                else:
                    dfs[i].to_csv(f"data/Dataset№{i + 1}.csv", index=False)
            else:
                if self.dataset_names != "":
                    dfs[i].to_parquet(f"data/{self.dataset_names[i]}.parquet", index=False)
                else:
                    dfs[i].to_parquet(f"data/Dataset№{i + 1}.parquet", index=False)
        dfs[-1].to_csv("data/Result.csv", index=False) if self.safe_to_csv else dfs[-1].to_parquet(
            "data/Result.parquet",
            index=False)

    def get_type(self, string):
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
                name1 = "dataset1" if self.dataset_names == "" else self.dataset_names[0]
                name2 = "dataset2" if self.dataset_names == "" else self.dataset_names[1]
                df1, df2 = self.generate_2_correlated_samples(schemas[i], schemas[i + 1], sizes[i], sizes[i + 1], name1,
                                                              name2,
                                                              correlated_keys[i])
                dfs.append(df1)
                dfs.append(df2)
                df = self.join_2_datasets(df1, df2, join_condition=join_conditions[i], join_type=join_type[i],i=i)
                for j in df1.columns.values:
                    typer = self.get_type(schemas[0][j].dataType)
                    if j in df.columns:
                        df[j] = df[j].astype(typer)
                    else:
                        df[j+f".{self.dataset_names[0]}"] = df[j+f".{self.dataset_names[0]}"].astype(typer)
                for j in df2.columns.values:
                    typer = self.get_type(schemas[1][j].dataType)
                    if j in df.columns:
                        df[j] = df[j].astype(typer)
                    else:
                        df[j + f".{self.dataset_names[1]}"] = df[j + f".{self.dataset_names[1]}"].astype(typer)
                df1 = df
            else:
                name = f"dataset{i}" if self.dataset_names == "" else self.dataset_names[i + 1]
                df = self.generate_sample_with_joined_sample(df1, schemas[i + 1], sizes[i + 1], name,
                                                             correlated_keys[i])
                dfs.append(df)
                df1 = self.join_2_datasets(df1, df, join_condition=join_conditions[i], join_type=join_type[i],i=i)
                for j in df.columns.values:
                    onlyField = j.split(".")[0]
                    typer = self.get_type(schemas[i + 1][onlyField].dataType)
                    df1[j] = df1[j].astype(typer)
        self.rename_results_dfs(df1)
        dfs.append(df1)
        self.save_dfs(dfs)
        return dfs

    def rename_results_dfs(self,df):
        need_to_change = {}
        for j in df.columns.values:
            j = j.split(".")
            if len(j) == 2:
                need_to_change[j[1]] = j[0]
        for key,value in need_to_change.items():
            df.rename(columns={f"{value}.{key}":f"{key}.{value}"},inplace=True)
