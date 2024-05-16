#import all the Libraries
import numpy as np
import pyhomogeneity as hg
import rasterio as ra
import time
import pandas as pd

from rasterio.plot import show
from rasterio.windows import Window
from rasterio.crs import CRS
from statistics import mode
import os
import itertools
from joblib import Parallel, delayed

## Input Data
n_rows = 114
n_col = 178
n_raster = 4017
path = r"E:/Data/"

## Function to perform petit test
def petit(x, y):
    print("Row :{} Column  : {}".format(x,y))
    try:
        result = hg.pettitt_test(stack[x, y, :])
        return x, y, result[1], result[2], result[3]
    except ValueError:
        print("Error : {} \n {}\n".format(x, y))
        return None  # Return None if there's an error


# List files in the file path directory
files = os.listdir(path)

# create an empty array for stacking and empty result array for cp, u, and p
stack = np.empty((n_rows, n_col, n_raster))

# Stack all the files in a 3D array
for fi in range(0, len(files)):
    fp = path + files[fi]
    raster = ra.open(fp)
    stack[:, :, fi] = raster.read(1)

print("Stacking Completed .... \n")
t_s = time.time()

# s_no2=0
l1 = 3
l2 = 3

# Use Parallel processing to speed up computation
results = Parallel(n_jobs=3)(delayed(petit)(x, y) for x in range(l1) for y in range(l2))

# Filter out None results
results = [res for res in results if res is not None]

over_x, over_y, over_cp, over_p, over_u = zip(*results)

#Store result in a csv file
out = pd.DataFrame({"X": over_x, "Y": over_y, "CP": over_cp, "P": over_p, "U": over_u})
out.to_csv("E:/Result.csv")

# Calculate mode of CP i.e day of change
change_point_mode = mode(over_cp)
print("Change point is {}\n".format(change_point_mode))
t_e = time.time()
print('TOTAL SIMULATION TIME: %f minutes = %f hours.' % ((t_e - t_s) / 60.0, (t_e - t_s) / 3600.0))
