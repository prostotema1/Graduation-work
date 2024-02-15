from sample_generator import sample_generator
import pandas as pd
from tabulate import tabulate


gen = sample_generator().init_with_cfg("../config.yaml")
df1 = pd.read_csv("user_buy")
df2 = pd.read_csv("user_info")
df4 = pd.read_csv("product")
df3 = pd.read_csv("Result")
print(tabulate(df2, headers=df2.keys(), tablefmt='psql'))
print(tabulate(df1, headers=df1.keys(), tablefmt='psql'))
print(tabulate(df4, headers=df4.keys(), tablefmt='psql'))
print(tabulate(df3, headers=df3.keys(), tablefmt='psql'))

#df1,df2,df3
#left,inner

#inner,left