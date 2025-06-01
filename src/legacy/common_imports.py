# Core packages
import sys
import logging
import os
import multiprocessing

# Core data handling
import pandas as pd  # type: ignore
import numpy as np# type: ignore
import dask.dataframe as dd # type: ignore
import pyarrow as pa # type: ignore
from pathlib import Path
from datetime import timedelta

# Geospatial operations
import geopandas as gpd # type: ignore
from shapely.geometry import Point, box # type: ignore

# Mapping
import folium # type: ignore

# Distance computation
from geopy.distance import geodesic # type: ignore

# Progress tracking
import tqdm # type: ignore

# Pretty printing of results
from pprint import pprint

# Visualization of statistics
import matplotlib.pyplot as plt # type: ignore