import boto3
import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import AutoMinorLocator

from config import graph_dir
from config import csv_save
from config import graph_path
from config import s3_bucket_name


def file_finder(change_path=csv_save):
    """
    based on a search parameter returns all matching files.
    :return: list of filepaths
    """
    os.chdir(change_path)
    file_search = 'instance_*/jstat_*.csv'
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
    ax3.set_title('FGCT Average').set_size(10)
    ax3.set_xlabel('hour', fontsize=10.5)
    ax3.set_ylabel('FGCT average (s)')

def grapher():
    """
    graphs to three subplots what the plotter has designated.
    :return: a pdf file
    """

    # ax1.xaxis.set_major_locator(DayLocator())
    ax1.tick_params(axis='x', direction='out', length=3, width=1, grid_alpha=0.5)
    ax2.tick_params(axis='x', direction='out', length=3, width=1, grid_alpha=0.5)
    ax3.tick_params(axis='x', direction='out', length=10, width=1, grid_alpha=0.5)

    fig.suptitle("Refined Jstat")


    ax3.tick_params(axis='x', which='major', labelsize=10.2)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%m'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=1))

    hours = mdates.HourLocator(interval=4)
    h_fmt = mdates.DateFormatter('%H')

    minor_locator = AutoMinorLocator(n=3)
    ax3.xaxis.set_minor_locator(minor_locator)
    ax3.xaxis.set_minor_formatter(h_fmt)
    plt.setp(ax1.xaxis.get_minorticklabels())



    if os.path.exists(graph_dir):
        os.chdir(graph_dir)
    else:
        os.mkdir(graph_dir)
        os.chdir(graph_dir)

    # layout
    axbox = ax3.get_position()
    fig.legend(loc='lower left', ncol=3,
               bbox_to_anchor=[0, axbox.y0 - 0.08, 1, 1],
               bbox_transform=fig.transFigure)

    plt.subplots_adjust(left=0.11,
                        hspace=0.343)

    fig.savefig('refined_jstat.png')


def s3_sender(s3_bucket=s3_bucket_name, graph_loc=graph_path):
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(
        graph_loc,  # file location
        s3_bucket,  # s3 bucket name
        "refined_jstat.png"  # object name
    )


if __name__ == '__main__':
    fig, (ax1, ax2, ax3) = plt.subplots(nrows=3, ncols=1, sharex=True, figsize=(16,10))
    for one_filename in file_finder():
        split_filename = (one_filename.split('/'))
        perpare_csv()
        plotter()
    grapher()
    try:
        s3_sender()
        print(f'file sent to {s3_bucket_name}')
    except:
        print('file transfer failed')