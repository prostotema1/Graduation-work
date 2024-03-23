import datetime

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
        dataset_sizes = {}
        schemas = {}
        not_unique_restrictions = {}
        unique_restrictions = {}
        possible_values = {}
        aggregation = {}
        for dataset in self.config['datasets']:
            dataset_names.append(dataset)
            schemas[dataset] = self.parse_fields(dataset)
            dataset_sizes[dataset] = self.config['datasets'][dataset]['size']
        for restriction in self.config['restrictions']:
            not_unique_restrictions[restriction] = self.config['restrictions'][restriction]
        for dataset in self.config['datasets']:
            alias = self.config['datasets'][dataset]["schema"]
            for field in alias:
                type = self.parse_field_type(dataset, field)
                if "restriction" in alias[field]:
                    value = list(alias[field]["restriction"].strip().split(":"))
                    if type == IntegerType():
                        value = list(map(lambda x: int(x), value))
                    elif type == DoubleType():
                        value = list(map(lambda x: float(x), value))
                    unique_restrictions[dataset + "." + field] = value
                if type == StringType() and "string_format" in alias[field] and "letters" in alias[field]:
                    unique_restrictions[dataset + "." + "field"] = [alias[field]["string_format"],
                                                                    alias[field]["letters"]]
                if "values" in alias[field]:
                    value = alias[field]['values'].split(",")
                    repeatable = alias[field]['repeatable']
                    if type == IntegerType():
                        value = list(map(lambda x: int(x), alias[field]['values'].split(",")))
                    if type == DoubleType():
                        value = list(map(lambda x: float(x), alias[field]['values'].split(",")))
                    possible_values[dataset + "." + field] = [repeatable, value]
        join_conditions = []
        join_type = []
        intersecting_keys = []
        for join in self.config['join_conditions']:
            join_conditions.append(self.config['join_conditions'][join]["condition"])
            join_type.append(self.config['join_conditions'][join]["type"])
            intersecting_keys.append(self.config['join_conditions'][join]["intersecting_records"])
            if 'where' in self.config['join_conditions'][join]:
                aggregation[self.config['join_conditions'][join]["condition"]] = self.config['join_conditions'][join][
                    "where"]
        inter = {}
        for i in range(len(intersecting_keys)):
            inter[dataset_names[i]] = intersecting_keys[i]
        self.check_restrictions(dataset_names, dataset_sizes, schemas, not_unique_restrictions, unique_restrictions,
                                possible_values, join_conditions, join_type, inter)
        dataset_sizes = list(map(lambda x: dataset_sizes[x], dataset_names))

        return dataset_names, dataset_sizes, schemas, not_unique_restrictions, unique_restrictions, possible_values, join_conditions, join_type, intersecting_keys, aggregation

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

    def check_restrictions(self,
                           dataset_names,
                           dataset_sizes,
                           schemas,
                           not_unique_restrictions,
                           unique_restrictions,
                           possible_values,
                           join_conditions,
                           join_type,
                           intersecting_keys):
        assert self.validate_sizes(schemas, join_type, join_conditions, intersecting_keys, dataset_sizes, dataset_names)
        assert self.validate_field_type(schemas, dataset_names)
        assert self.validate_size_and_not_unique_restriction(dataset_names, schemas, not_unique_restrictions,
                                                             unique_restrictions, possible_values, dataset_sizes)
        assert self.validate_size_and_unique_restrictions_for_dataset(dataset_names, dataset_sizes, schemas,
                                                                      unique_restrictions)
        assert self.validate_size_and_unique_restrictons(dataset_names, dataset_sizes, schemas, unique_restrictions,
                                                         intersecting_keys)
        assert self.validate_size_and_possible_values(dataset_names, schemas, intersecting_keys, possible_values)
        assert self.validate_field_size_with_possible_values(dataset_names, schemas, dataset_sizes, possible_values)
        assert self.validate_fields_size_with_possible_values_and_unique_restrictions(dataset_names, schemas,
                                                                                      intersecting_keys,
                                                                                      unique_restrictions,
                                                                                      possible_values,
                                                                                      not_unique_restrictions)

    def validate_field_type(self, schemas, dataset_names):
        for i in range(len(dataset_names) - 1):
            dataset1 = dataset_names[i]
            for j in range(i + 1, len(dataset_names)):
                dataset2 = dataset_names[j]
                for field in schemas[dataset1].fields:
                    if field in schemas[dataset2].fields:
                        field2 = schemas[dataset2].fields
                        fieldd = None
                        for x in field2:
                            if x.name == field.name:
                                fieldd = x
                                break
                        if fieldd.dataType != field.dataType:
                            return False
        return True

    def validate_sizes(self, schemas, join_type, join_conditions, correlated_keys, dataset_size, dataset_names) -> bool:
        if len(schemas) == len(join_type) + 1 and len(schemas) == len(join_conditions) + 1 and len(schemas) == len(
                correlated_keys) + 1:
            for i in range(len(correlated_keys) - 1):
                if correlated_keys[dataset_names[i]] < correlated_keys[dataset_names[i + 1]]:
                    return False
                if dataset_size[dataset_names[i]] < correlated_keys[dataset_names[i]]:
                    return False
            return True
        return False

    def validate_size_and_not_unique_restriction(self, dataset_names, schemas, not_unique_restrictions,
                                                 unique_restrictions, possible_values, dataset_sizes) -> bool:
        for dataset in dataset_names:
            for field in schemas[dataset].fields:
                if f"{dataset}.{field.name}" not in unique_restrictions and f"{dataset}.{field.name}" not in possible_values:
                    if field.dataType == IntegerType():
                        if not_unique_restrictions['max_int'] - not_unique_restrictions['min_int'] < dataset_sizes[
                            dataset]:
                            return False
                    if field.dataType == DateType():
                        delta = (datetime.date(not_unique_restrictions['max_date']) - datetime.date(
                            not_unique_restrictions['max_date'])).days
                        if delta < dataset_sizes[dataset]:
                            return False
                    if field.dataType == DoubleType():
                        l = not_unique_restrictions["min_double"]
                        r = not_unique_restrictions["max_double"]
                        if r - l <= 0:
                            return False
        return True

    def validate_size_and_possible_values(self, dataset_names, schemas, correlated_keys, possible_values) -> bool:
        for i in range(len(dataset_names) - 1):
            dataset1 = dataset_names[i]
            dataset2 = dataset_names[i + 1]
            for field in schemas[dataset1].fields:
                if f"{dataset1}.{field.name}" in possible_values and f"{dataset2}.{field.name}" in possible_values:
                    restr1 = possible_values[f"{dataset1}.{field.name}"][1]
                    restr2 = possible_values[f"{dataset2}.{field.name}"][1]
                    intersection = [x for x in restr1 if x in restr2]
                    if len(intersection) < correlated_keys[i] and (not possible_values[f"{dataset1}.{field.name}"][0] \
                                                                   or not possible_values[f"{dataset1}.{field.name}"][
                                0]):
                        return False
        return True

    def validate_field_size_with_possible_values(self, dataset_names, schemas, dataset_sizes, possible_values) -> bool:
        for dataset in dataset_names:
            for field in schemas[dataset].fields:
                if f"{dataset}.{field.name}" in possible_values and len(possible_values[f"{dataset}.{field.name}"][1]) < \
                        dataset_sizes[dataset] and not possible_values[f"{dataset}.{field.name}"][0]:
                    return False
        return True

    def validate_fields_size_with_possible_values_and_unique_restrictions(self, dataset_names, schemas, correlated_keys,
                                                                          unique_restrictions, possible_values,
                                                                          not_unique_restrictions):
        for i in range(len(dataset_names) - 1):
            dataset1 = dataset_names[i]
            dataset2 = dataset_names[i + 1]
            for field in schemas[dataset1].fields:
                ans1 = self.helper_unique_and_possible(i, dataset1, dataset1, dataset2, correlated_keys,
                                                       unique_restrictions,
                                                       possible_values, field)
                ans2 = self.helper_unique_and_possible(i, dataset1, dataset2, dataset1, correlated_keys,
                                                       unique_restrictions,
                                                       possible_values,
                                                       field)
                ans3 = self.helper_unique_and_not_unique(i, dataset1, dataset1, dataset2, correlated_keys,
                                                         unique_restrictions,
                                                         not_unique_restrictions, possible_values, field)
                ans4 = self.helper_unique_and_not_unique(i, dataset1, dataset2, dataset1, correlated_keys,
                                                         unique_restrictions,
                                                         not_unique_restrictions, possible_values, field)
                ans5 = self.helper_not_unique_and_possible(i, dataset1, dataset1, dataset2, correlated_keys,
                                                           unique_restrictions, not_unique_restrictions,
                                                           possible_values, field)
                ans6 = self.helper_not_unique_and_possible(i, dataset1, dataset2, dataset1, correlated_keys,
                                                           unique_restrictions, not_unique_restrictions,
                                                           possible_values, field)

                if not ans1 or not ans2 or not ans3 or not ans4 or not ans5 or not ans6:
                    return False
        return True

    def helper_unique_and_possible(self, i, fixed_name, dataset_name1, dataset_name2, correlated_keys,
                                   unique_restrictions,
                                   possible_values, field) -> bool:
        if f"{dataset_name1}.{field.name}" in possible_values and f"{dataset_name2}.{field.name}" in unique_restrictions:
            if field.dataType == IntegerType() or field.dataType == DoubleType():
                counter = 0
                l = unique_restrictions[f"{dataset_name2}.{field.name}"][0]
                r = unique_restrictions[f"{dataset_name2}.{field.name}"][1]
                for x in possible_values[f"{dataset_name1}.{field.name}"][1]:
                    if l <= x <= r:
                        counter += 1
                if counter < correlated_keys[fixed_name] and not possible_values[f"{dataset_name1}.{field.name}"][0]:
                    return False
            elif field.dataType == DateType():
                counter = 0
                l = unique_restrictions[f"{dataset_name2}.{field.name}"][0].split("-")
                l = datetime.date(int(l[0]), int(l[1]), int(l[2]))
                r = unique_restrictions[f"{dataset_name2}.{field.name}"][1].split("-")
                r = datetime.date(int(r[0]), int(r[1]), int(r[2]))
                for x in possible_values[f"{dataset_name1}.{field.name}"][1]:
                    x = x.split("-")
                    x = datetime.date(int(x[0]), int(x[1]), int(x[2]))
                    if l <= x <= r:
                        counter += 1
                if counter < correlated_keys[fixed_name] and not possible_values[f"{dataset_name1}.{field.name}"][0]:
                    return False
        return True

    def helper_unique_and_not_unique(self, i, fixed_name, dataset_name1, dataset_name2, correlated_keys,
                                     unique_restrictions,
                                     not_unique_restrictions, possible_values, field) -> bool:
        if f"{dataset_name1}.{field.name}" in unique_restrictions and f"{dataset_name2}.{field.name}" not in unique_restrictions and f"{dataset_name2}.{field.name}" not in possible_values:
            if field.dataType == IntegerType():
                l1 = unique_restrictions[f"{dataset_name1}.{field.name}"][0]
                r1 = unique_restrictions[f"{dataset_name1}.{field.name}"][1]
                l2 = not_unique_restrictions['min_int']
                r2 = not_unique_restrictions['max_int']
                if l1 <= l2 <= r1 <= r2:
                    if r1 - l2 < correlated_keys[dataset_name1]:
                        return False
                elif r1 >= l2 >= l1 and r2 <= r1:
                    if r2 - l2 < correlated_keys[dataset_name1]:
                        return False
                elif l2 <= l1 and r2 <= r1:
                    if r2 - l1 < correlated_keys[dataset_name1]:
                        return False
                elif l2 >= r2 or r2 <= l1:
                    if l2 == r2 or r2 == l1:
                        if correlated_keys[dataset_name1] != 1:
                            return False
                    else:
                        return False
                elif l1 >= l2 and r1 <= r2:
                    if r1 - l1 < correlated_keys[dataset_name1]:
                        return False
            if field.dataType == DoubleType():
                l1 = unique_restrictions[f"{dataset_name1}.{field.name}"][0]
                r1 = unique_restrictions[f"{dataset_name1}.{field.name}"][1]
                l2 = not_unique_restrictions['min_double']
                r2 = not_unique_restrictions['max_double']
                if l2 >= r1 or r2 <= l1:
                    if l2 == r1 or r2 <= l1:
                        if correlated_keys[fixed_name] != 1:
                            return False
                    else:
                        return False
            if field.dataType == DateType():
                l1 = unique_restrictions[f"{dataset_name1}.{field.name}"][0].split("-")
                l1 = datetime.date(int(l1[0]), int(l1[1]), int(l1[2]))
                r1 = unique_restrictions[f"{dataset_name1}.{field.name}"][1].split("-")
                r1 = datetime.date(int(r1[0]), int(r1[1]), int(r1[2]))
                l2 = not_unique_restrictions['min_date']
                r2 = not_unique_restrictions['max_date']
                if l2 >= r1 or r2 <= l1:
                    if l2 == r1 or r2 == l1:
                        if correlated_keys[fixed_name] != 1:
                            return False
                    else:
                        return False
                if l1 <= l2 <= r1 <= r2:
                    if (r1 - l2).days < correlated_keys[fixed_name]:
                        return False
                elif l1 <= l2 <= r2 <= r1:
                    if (r2 - l2).days < correlated_keys[fixed_name]:
                        return False
                elif l2 <= l1 <= r2 <= r1:
                    if (r2 - l1).days < correlated_keys[fixed_name]:
                        return False
                elif l2 <= l1 <= r1 <= r2:
                    if (r1 - l1).days < correlated_keys[fixed_name]:
                        return False

        return True

    def helper_not_unique_and_possible(self, i, fixed_name, dataset1, dataset2, correlated_keys, unique_restrictions,
                                       not_unique_restrictions, possible_values, field) -> bool:
        if f"{dataset1}.{field.name}" in possible_values and f"{dataset2}.{field.name}" not in possible_values and f"{dataset2}.{field.name}" not in unique_restrictions:
            if field == IntegerType():
                l = unique_restrictions['min_int']
                r = unique_restrictions['max_int']
                counter = 0
                for x in possible_values[f"{dataset1}.{field.name}"][1]:
                    if l <= x <= r:
                        counter += 1
                        if counter == correlated_keys[fixed_name]:
                            break
                if counter < correlated_keys[fixed_name] and not possible_values[f"{dataset1}.{field.name}"][1]:
                    return False
            elif field == DoubleType():
                l = unique_restrictions['min_double']
                r = unique_restrictions['max_double']
                counter = 0
                for x in possible_values[f"{dataset1}.{field.name}"][1]:
                    if l <= x <= r:
                        counter += 1
                        if counter == correlated_keys[fixed_name]:
                            break
                if counter < correlated_keys[fixed_name] and not possible_values[f"{dataset1}.{field.name}"][0]:
                    return False
            elif field == DateType():
                l = unique_restrictions['min_date'].split("-")
                r = unique_restrictions['max_date'].split("-")
                l = datetime.date(int(l[0]), int(l[1]), int(l[2]))
                r = datetime.date(int(r[0]), int(r[1]), int(r[2]))
                counter = 0
                for x in possible_values[f"{dataset1}.{field.name}"][1]:
                    x = x.split("-")
                    x = datetime.date(int(x[0]), int(x[1]), int(x[2]))
                    if l <= x <= r:
                        counter += 1
                    if counter == correlated_keys[fixed_name]:
                        break
                if counter < correlated_keys[fixed_name] and not possible_values[f"{dataset1}.{field.name}"][0]:
                    return False
        return True

    def validate_size_and_unique_restrictons(self, dataset_names, dataset_sizes, schemas, unique_restrictions,
                                             correlated_keys) -> bool:
        for i in range(len(dataset_names) - 1):
            dataset1 = dataset_names[i]
            dataset2 = dataset_names[i + 1]
            for field1 in schemas[dataset1]:
                if f"{dataset1}.{field1.name}" in unique_restrictions and f"{dataset2}.{field1.name}" in unique_restrictions:
                    if (field1.dataType == StringType() and
                            unique_restrictions[f"{dataset1}.{field1.name}"] != unique_restrictions[
                                f"{dataset2}.{field1.name}"]):
                        return False
                    if field1.dataType == IntegerType():
                        if unique_restrictions[f"{dataset1}.{field1.name}"] == unique_restrictions[
                            f"{dataset2}.{field1.name}"]:
                            leng = unique_restrictions[f"{dataset1}.{field1.name}"][1] - \
                                   unique_restrictions[f"{dataset1}.{field1.name}"][0]
                            if leng < correlated_keys[dataset1]:
                                return False
                        else:
                            l1 = unique_restrictions[f"{dataset1}.{field1.name}"][0]
                            r1 = unique_restrictions[f"{dataset1}.{field1.name}"][1]
                            l2 = unique_restrictions[f"{dataset2}.{field1.name}"][0]
                            r2 = unique_restrictions[f"{dataset2}.{field1.name}"][1]
                            if l1 <= l2 <= r1 <= r2:
                                if r1 - l2 < correlated_keys[dataset1]:
                                    return False
                            elif r1 >= l2 >= l1 and r2 <= r1:
                                if r2 - l2 < correlated_keys[dataset1]:
                                    return False
                            elif l2 <= l1 and r2 <= r1:
                                if r2 - l1 < correlated_keys[dataset1]:
                                    return False
                            elif l2 >= r2 or r2 <= l1:
                                if l2 == r2 or r2 == l1:
                                    if correlated_keys[dataset1] != 1:
                                        return False
                                else:
                                    return False
                            elif l1 >= l2 and r1 <= r2:
                                if r1 - l1 < correlated_keys[dataset1]:
                                    return False
                    if field1.dataType == DoubleType():
                        l1 = unique_restrictions[f"{dataset1}.{field1.name}"][0]
                        r1 = unique_restrictions[f"{dataset1}.{field1.name}"][1]
                        l2 = unique_restrictions[f"{dataset2}.{field1.name}"][0]
                        r2 = unique_restrictions[f"{dataset2}.{field1.name}"][1]
                        if l2 >= r1 or r2 <= l1:
                            if l2 == r1 or r2 <= l1:
                                if correlated_keys[dataset1] != 1:
                                    return False
                            else:
                                return False

                    if field1.dataType == DateType():
                        l1 = unique_restrictions[f"{dataset1}.{field1.name}"][1].split("-")
                        l1 = datetime.date(int(l1[0]), int(l1[1]), int(l1[2]))
                        r1 = unique_restrictions[f"{dataset1}.{field1.name}"][0].split("-")
                        r1 = datetime.date(int(r1[0]), int(r1[1]), int(r1[2]))
                        l2 = unique_restrictions[f"{dataset2}.{field1.name}"][1].split("-")
                        l2 = datetime.date(int(l2[0]), int(l2[1]), int(l2[2]))
                        r2 = unique_restrictions[f"{dataset2}.{field1.name}"][0].split("-")
                        r2 = datetime.date(int(r2[0]), int(r2[1]), int(r2[2]))
                        if l2 >= r1 or r2 <= l1:
                            if l2 == r1 or r2 == l1:
                                if correlated_keys[dataset1] != 1:
                                    return False
                            else:
                                return False
                        if l1 <= l2 <= r1 <= r2:
                            if (r1 - l2).days < correlated_keys[dataset1]:
                                return False
                        elif l1 <= l2 <= r2 <= r1:
                            if (r2 - l2).days < correlated_keys[dataset1]:
                                return False
                        elif l2 <= l1 <= r2 <= r1:
                            if (r2 - l1).days < correlated_keys[dataset1]:
                                return False
                        elif l2 <= l1 <= r1 <= r2:
                            if (r1 - l1).days < correlated_keys[dataset1]:
                                return False

        return True

    def validate_size_and_unique_restrictions_for_dataset(self, dataset_names, dataset_sizes, schemas,
                                                          unique_restrictions) -> bool:
        for i in range(len(dataset_names) - 1):
            dataset1 = dataset_names[i]
            for field1 in schemas[dataset1]:
                if f"{dataset1}.{field1.name}" in unique_restrictions:
                    if field1.dataType == IntegerType():
                        r = unique_restrictions[f"{dataset1}.{field1.name}"][1]
                        l = unique_restrictions[f"{dataset1}.{field1.name}"][0]
                        if r - l < dataset_sizes[dataset1]:
                            return False
                    if field1.dataType == DateType():
                        date1 = unique_restrictions[f"{dataset1}.{field1.name}"][1].split("-")
                        date1 = datetime.date(int(date1[0]), int(date1[1]), int(date1[2]))
                        date2 = unique_restrictions[f"{dataset1}.{field1.name}"][0].split("-")
                        date2 = datetime.date(int(date2[0]), int(date2[1]), int(date2[2]))
                        if (date1 - date2).days < dataset_sizes[dataset1]:
                            return False
                    if field1.dataType == DoubleType():
                        l = unique_restrictions[f"{dataset1}.{field1.name}"][0]
                        r = unique_restrictions[f"{dataset1}.{field1.name}"][1]
                        if r - l <= 0:
                            return False
        return True
