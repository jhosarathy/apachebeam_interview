import argparse
import csv
import json
import logging
import re
from typing import Any, Dict

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.options.pipeline_options import PipelineOptions
from pydantic import BaseModel

timestamp_re = re.compile(r'(\d{4})\-(\d{2})\-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})\s+(\w+)')

class Transaction(BaseModel):
    timestamp: str
    origin: str
    destination: str
    transaction_amount: str
    derived_year: int = -1
    derived_amount: float = -1
    derived_date: str = ""
    
def parse_file(element) -> Dict[str, Any]:
    for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL):
        line = [s.replace('\"', '') for s in line]
        t = Transaction(timestamp=line[0], origin=line[1], destination=line[2], transaction_amount=line[3])
        if(t.timestamp != "timestamp"):
            t.derived_year = int(timestamp_re.sub(r'\1', t.timestamp))
            t.derived_amount = float(t.transaction_amount)
            t.derived_date = timestamp_re.sub(r'\1-\2-\3', t.timestamp)
        return t.dict(exclude={'timestamp_re'}, include={
            'timestamp',
            'origin',
            'destination',
            'transaction_amount',
            'derived_year',
            'derived_amount',
            'derived_date'
        })

def is_amt_gt_20(t: Dict[str, Any]) -> bool:
    return t['derived_amount'] > 20

def is_year_after_2010(t: Dict[str, Any]) -> bool:
    return t['derived_year'] >= 2010

def to_dict(t: Dict[str, Any]) -> Dict[str, Any]:
    return t.dict(exclude={'timestamp_re'},include={
        'timestamp',
        'origin',
        'destination',
        'transaction_amount',
        'derived_year',
        'derived_amount',
        'derived_date'
    })
    
    
def task1(input, output ):
    options = PipelineOptions(
        runner='DirectRunner',
        input_file=input
    )

    pipeline = beam.Pipeline(options=options)

    clean_csv = (pipeline 
    | 'read input file' >> beam.io.ReadFromText(input)
    | 'parse file' >> beam.Map(parse_file)
    | 'amt > 20' >> beam.Filter(is_amt_gt_20)
    | 'after 2010' >> beam.Filter(is_year_after_2010)
    | 'to beam row' >> beam.Map(lambda row: beam.Row(**row))
    | 'group by' >> beam.GroupBy('derived_date').aggregate_field('derived_amount', sum, 'total_amount')
    | 'format output' >> beam.Map(lambda row: json.dumps(row._asdict()))
    | 'write output' >> beam.io.WriteToText(output,num_shards=1,compression_type=CompressionTypes.GZIP, file_name_suffix='.gz')
    )
   
    pipeline.run()

if __name__ == '__main__':
   import argparse

   # Create the parser  
   parser = argparse.ArgumentParser(description='Run the CSV cleaning pipeline')   

   parser.add_argument('-f','--input-file', help='Input GCS file path', required=True)
   parser.add_argument('-o','--output-file', help='Output CSV file path', required=True)
   
   # Execute the parse_args() method
   args = vars(parser.parse_args())

   task1(input=args['input_file'], output=args['output_file'])

