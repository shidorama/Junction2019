import pandas as pd
import pyproj
from pyproj import CRS
import json

data = pd.read_csv("routes.csv", sep=";", encoding='latin1')

arra = data.as_matrix()

print(arra)