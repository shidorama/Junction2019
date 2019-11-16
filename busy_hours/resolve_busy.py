import pandas as pd
from datetime import datetime

df = pd.read_csv("result_holiday.csv")

df["startDate"].dropna().apply(lambda x: datetime.fromtimestamp(float(x)))

print(len(df))