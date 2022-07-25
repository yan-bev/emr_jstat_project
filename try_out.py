import pandas as pd
from pathlib import Path
# test


df = pd.read_csv("C:\\Users\yaniv\Documents\get-a-job\projects\emr_jstat\jstat_outputs\jstat_Ide7a6_pid10772.csv")
# print(df.loc[[0], ['O', 'FGC']])  # this prints by the label (the index)

df.drop(df[df['O'] == 'O'].index, inplace=True)
# this changes the whole file to floats, otherwise, i wouldn't be able to do math
df_float = df.astype(float)

# def :: from here down, we print out the 'O' values when FGCT has changed.
# this adds change of FGCT column and sees where there was a difference
df_float['\u0394FGCT'] = df_float['FGCT'].diff()

# here if the difference is over zero (excluding NAN values) we save the row as df_change
df_change = df_float[df_float['\u0394FGCT'] > 0]
print(df_float)
# this prints out the O values everytime
print(df_change.O)
