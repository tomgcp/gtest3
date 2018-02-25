
"""
Copyright Google Inc. 2018
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import argparse
import logging
import time
import datetime, os

import apache_beam as beam
from apache_beam import window
import apache_beam.transforms.window as window
from apache_beam.io.gcp.internal.clients import bigquery


'''

This is ...

@author tomstern
based on original work by vlakshmanan

  1) Enable Dataflow API
  2) Create PubSub 'streamdemo' topic
  3) Create Bigquery dataset 'demos'

python StreamingConsumer.py --project <PROJECT> --bucket <BUCKET> --DirectRunner or --DataFlowRunner

'''

# Provide intant timestamps
#
class AddTimestampDoFn(beam.DoFn):
  def process(self, element):
     yield beam.window.TimestampedValue(element,int(datetime.datetime.now().strftime('%s') ))

def countwords(line):
   if len(line)==0:
      return 0
   words=line.split(' ')
   return len(words)


# ### main 

# Define pipeline runner (lazy execution)
def run():

# Command line arguments
  parser = argparse.ArgumentParser(description='Demonstrate side inputs')
  parser.add_argument('--bucket', required=True, help='Specify Cloud Storage bucket for output')
  parser.add_argument('--project',required=True, help='Specify Google Cloud project')
  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument('--DirectRunner',action='store_true')
  group.add_argument('--DataFlowRunner',action='store_true')
      
  opts = parser.parse_args()

  if opts.DirectRunner:
    runner='DirectRunner'
  if opts.DataFlowRunner:
    runner='DataFlowRunner'

  bucket = opts.bucket
  project = opts.project

  argv = [
    '--project={0}'.format(project),
    '--job_name=streamingjob',
    '--save_main_session',
    '--staging_location=gs://{0}/staging/'.format(bucket),
    '--temp_location=gs://{0}/staging/'.format(bucket),
    '--runner={0}'.format(runner)
    ]

  pubsubinput='projects/{0}/topics/streamdemo'.format(project)
  pubsubschema = 'datetime:TIMESTAMP, num_words:INTEGER'
  
  p = beam.Pipeline(argv=argv)
  (p 
        | 'ReadFromPubSub' >> beam.io.ReadStringsFromPubSub( pubsubinput )
        | 'WordsPerMessage' >> beam.Map(lambda msg: (msg,countwords(msg)) )
#       | 'timestamp' >> beam.ParDo(AddTimestampDoFn())          # Tried adding an istantaneous timestamp
#       | 'window' >> beam.WindowInto(window.FixedWindows(60))   # Tried fixed window
        | 'Window' >> beam.WindowInto(window.SlidingWindows(1, 15))     
        | 'WordsInTimeWindow' >> beam.Map(lambda words: {'datetime': int(datetime.datetime.now().strftime('%s')),'num_words':sum(words)}) 
        | 'Combine' >> beam.CombineGlobally(sum).without_defaults()
        | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(    
          project=project, dataset='demos',table='streamdemo',
          schema=pubsubschema,
          write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
  ) 

  if runner == 'DataFlowRunner':
     p.run()
  else:
     p.run().wait_until_finish()
  logging.getLogger().setLevel(logging.INFO)


if __name__ == '__main__':
  run()

