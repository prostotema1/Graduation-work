import yaml
from pyspark.sql.types import IntegerType, DoubleType, DateType, BooleanType, StringType, StructType, StructField


class Config_Manager:

    def __init__(self, filepath: str):
        try:
            with open(filepath, 'r') as f:
                self.config = yaml.safe_load(f)
        except Exception:
            raise Exception("Something wrong with configuration file")
        self.give_info()

    def give_info(self):
        dataset_names = []
        dataset_sizes = []
        schemas = {}
        restrictions = {}
        for dataset in self.config['datasets']:
            dataset_names.append(dataset)
            schemas[dataset] = self.parse_fields(dataset)
            dataset_sizes.append(self.config['datasets'][dataset]['size'])
        for restriction in self.config['restrictions']:
            restrictions[restriction] = self.config['restrictions'][restriction]
        join_conditions = []
        join_type = []
        intersecting_keys = []
        for join in self.config['join_conditions']:
            join_conditions.append(self.config['join_conditions'][join]["condition"])
            join_type.append(self.config['join_conditions'][join]["type"])
            intersecting_keys.append(self.config['join_conditions'][join]["intersecting_records"])
        return dataset_names, dataset_sizes, schemas, restrictions, join_conditions,join_type,intersecting_keys

    def parse_fields(self, dataset):
        schema = StructType()
        for field in self.config['datasets'][dataset]['schema']:
            field_name = field
            field_type = self.parse_field_type(dataset, field_name)
            is_nullable = self.config['datasets'][dataset]['schema'][field_name]["nullable"]
            schema.add(StructField(field_name, field_type, is_nullable))
        return schema

    def parse_field_type(self, dataset, field):
        field_temp = self.config['datasets'][dataset]['schema'][field]['type']
        if field_temp == 'IntegerType':
            return IntegerType()
        elif field_temp == 'DoubleType':
            return DoubleType()
        elif field_temp == 'DateType':
            return DateType()
        elif field_temp == 'BooleanType':
            return BooleanType()
        else:
            return StringType()

