import boto3
import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DayLocator
import matplotlib.dates as mdates
from matplotlib.ticker import AutoMinorLocator

from config import graph_parent_save_location
from config import master_directory_path_for_df_save
from config import full_graph_save_path
from config import s3_bucket_name

s3_b = s3_bucket_name
CHANGE_PATH = master_directory_path_for_df_save
SAVE_PATH = graph_parent_save_location
FULL_SAVE = full_graph_save_path

def file_finder(directory_path=CHANGE_PATH):
    """
    based on a search parameter returns all matching files.
    :param directory_path:
    :return: list of filepaths
    """
    file_search = '/tmp/jstat_output/instance_*/jstat_*.csv'
    finder = glob.glob(file_search, recursive=True)
    return(finder)

def perpare_csv():
    """
    prepares the CSV for graphing: removes the first unwanted row, removes any problematic jstat outputs
    (jstat column jumbling), and adds change-of columns.
    :param csv_file:
    :return: dataframe
    """
    df = pd.read_csv(one_filename)
    df.drop(df[df['O'] == 'O'].index, inplace=True)
    # cleans up the csv
    df = (df.iloc[1:])
    df.columns = ["DateTime", "O", "FGC", "FGCT"]
    # checks for incorrect data
    FGC_df = df.FGC.astype(str)
    df['FGC'] = FGC_df
    df = df[df['FGC'].str.contains(r'.', regex=False) == False]  # this removes unwanted columns

    # change columns to floats
    FGCT_df = pd.to_numeric(df["FGCT"])  # sets to float64
    df['FGCT'] = FGCT_df
    O_df = pd.to_numeric((df["O"]))  # sets to float64
    df["O"] = O_df
    FGC_df = pd.to_numeric(df["FGC"])  # sets to float64
    df["FGC"] = FGC_df
    df['DateTime'] = pd.to_datetime(df.DateTime)

    # add change of FGC, FGCT and  FGCT/FGC columns

    df['FGCTFGC'] = df['FGCT'] / df['FGC']
    df['\u0394FGCT'] = df['FGCT'].diff().fillna(0)
    df['\u0394FGC'] = df['FGC'].diff().fillna(0)

    return df


def plotter():
    """
    plots the dataframe to three graphs; O value following FGC,
    number of FGC, and FGCT average time (s).
    :param DataFrame:
    :return: pdf?
    """
    df = perpare_csv()
    fgc_change = df[df['\u0394FGC'] != 0]
    fgct_change = df[df['\u0394FGCT'] != 0]

    # plots number of FGC every hour
    ax1.plot(df.DateTime, df.FGC, marker='.')
    ax1.set_title('FGC count').set_size(10)
    ax1.set_ylabel('FGC count')

    # plots O value when FGC changes
    ax2.plot(fgct_change.DateTime, fgct_change.O, marker='.')
    ax2.set_title('O % following FGC').set_size(10)
    ax2.set_ylabel('O Value %')

   # plot the FGCT Average
    ax3.plot(df.DateTime, df.FGCTFGC, marker='.',
             label=f'{split_filename[0][-4:]}_{split_filename[1][-8:-4]}')
    ax3.legend(bbox_to_anchor=(0.5, -0.5), fontsize=7, loc='upper center', ncol=2)
    ax3.set_title('FGCT Average').set_size(10)
    ax3.set_xlabel('hour')
    ax3.set_ylabel('FGCT average (s)')

def grapher():
    """
    graphs to three subplots what the plotter has designated.
    :return: a pdf file
    """

    # hours = mdates.HourLocator(byhour=[0, 6])  # every 6 hours
    h_fmt = mdates.DateFormatter('%H')

    ax1.xaxis.set_major_locator(DayLocator())
    ax1.tick_params(axis='x', direction='out', length=3, width=1, grid_alpha=0.5)
    ax2.tick_params(axis='x', direction='out', length=3, width=1, grid_alpha=0.5)
    ax3.tick_params(axis='x', direction='out', length=8, width=1, grid_alpha=0.5)

    ax3.get_shared_x_axes().join(ax1, ax2, ax3)
    ax3.xaxis.set_minor_locator(AutoMinorLocator(n=4))
    ax3.xaxis.set_minor_formatter(h_fmt)
    fig.suptitle("Refined Jstat")
    plt.setp(ax1.get_xticklabels(), visable=False)
    plt.setp(ax2.get_xticklabels(), visable=False)
    plt.subplots_adjust(left=0.11,
                        hspace=0.343)

    if os.path.exists(SAVE_PATH):
        os.chdir(SAVE_PATH)
    else:
        os.mkdir(SAVE_PATH)
        os.chdir(SAVE_PATH)
    fig.legend(bbox_transform=fig.transFigure)
    fig.savefig('refined_jstat.png', bbox_inches='tight')

def s3_sender(s3_bucket=s3_b):
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(
        FULL_SAVE,  # file location
        s3_bucket,  # s3 bucket name
        "refined_jstat.png"  # object name
    )


if __name__ == '__main__':
    os.chdir(CHANGE_PATH)
    fig, (ax1, ax2, ax3) = plt.subplots(nrows=3, ncols=1) #sharex=True
    for count, one_filename in enumerate(file_finder()):
        split_filename = (one_filename.split('/'))
        print('split files')
        perpare_csv()
        print('csv perpared')
        plotter()
        print('plot finished')
    grapher()
    print('graphed')
    s3_sender()
    print('object sent')
    #TODO: Fix Legend - added fig.legend (bbox_inches causing problem)
    #TODO: create .sh file and add here!
