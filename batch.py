import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv
import os
import numpy as np
import pandas as pd 
import apache_beam.runners.interactive.interactive_beam as ib
from apache_beam import dataframe
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
from apache_beam.runners.dataflow import DataflowRunner
from google.oauth2 import service_account

PROJECT_ID = 'flash-datum-368412'
SCHEMA = 'Company:STRING,TypeName:STRING,Inches:STRING,ScreenResolution:STRING,Cpu:STRING,Ram:FLOAT,Memory:STRING,Gpu:STRING,OpSys:STRING,Weight:FLOAT,Price:FLOAT'

def filter_price(data):
    """Filters out records that have a negative price."""
    return data['Price'] > 0


def convert_types(data):
    """Delete GB and kg and Converts string values to their appropriate type."""
    
    data['Ram'] = data['Ram'].replace('GB','')
    data['Weight'] = data['Weight'].replace('kg','')
    # converting from string->integer for ram column
    data['Ram'] = float(data['Ram'])
    # converting from string-> float for the weight column

    data['Weight'] = float(data['Weight'])
    data['Price'] = float(data['Price'])
    
    return data

def del_unwanted_cols(data):
    """Delete the unwanted columns"""
    del data['Unnamed: 0']
    return data


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions())

    (p | 'ReadData' >> beam.io.ReadFromText('gs://cours-nizar-test-data/laptop/laptop_data.csv', skip_header_lines =1)
       | 'SplitData' >> beam.Map(lambda x: x.split(','))
       | 'FormatToDict' >> beam.Map(lambda x: {"Unnamed: 0": x[0], "Company": x[1], "TypeName": x[2], "Inches": x[3], "ScreenResolution": x[4], "Cpu": x[5], "Ram": x[6], "Memory": x[7], "Gpu": x[8], "OpSys": x[9], "Weight": x[10], "Price": x[11]}) 
       | 'DeleteIncompleteData' >> beam.Map(del_unwanted_cols)
       | 'ChangeDataType' >> beam.Map(convert_types)
       | 'FilterPrice'  >> beam.Filter(filter_price)
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:laptop.data_laptop2'.format(PROJECT_ID),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    result = p.run()
    result.wait_until_finish()




