import os
import pandas as pd


full_path = os.path.abspath('../Upload/csv/')
dirname = '../Upload/csv/'
dirfiles = os.listdir(dirname)

fullpaths = map(lambda name: os.path.join(full_path, name), dirfiles)

dirs = []
files = []

print([file for file in fullpaths if os.path.isfile(file)])


