# Quickstart: Apache Beam & Google Cloud Dataflow (Python)

*github.com/heerman*


## Concept

Run a your first Apache Beam pipeline, quickly, with Google Dataflow and Google Cloud Storage


## 1. Setup Google Account for Google Cloud Platform

* Configure Google Cloud Platform as described by the Google tutorial (and outlined below)

    * https://cloud.google.com/dataflow/docs/quickstarts/quickstart-python

* Sign up for an account on cloud.google.com

* Create a GCP project

* Enable billing (aka give Google your credit card)

* Enable APIs

    * Cloud Dataflow, Compute Engine, Stackdriver Logging, Google Cloud Storage, Google Cloud Storage JSON, BigQuery, Google Cloud Pub/Sub, Google Cloud Datastore, Google Cloud Resource Manager

* Create JSON credentials for service account, so apps can reach your storage bucket

    * Set environmental variable `GOOGLE_APPLICATION_CREDENTIALS=/path/to/json/credential/file`

* Create 2 Google storage buckets


## 2. Install Apache Beam for Python

* Follow this Apache Beam tutorial (outlined below)

    * https://beam.apache.org/get-started/quickstart-py/

* Install Python 2.7

* Install pip (MacOs below)

    * `sudo easy_install pip`

    * `pip --version`

* Install virtualenv, setuptools

    * `sudo install —upgrade virtualenv`

    * `sudo install —upgrade setuptools`

* Create a Python virtual environment

    * `cd ~/Code/UMG_Technical/`

    * `mkdir umgtechnical-env`

    * `virtualenv umgtechnical-env/`

    * `. umgtechnical-env/bin/activate`

* Note, close the virtual-env with `deactivate`, when you're done

* Install apache-beam, and Google Cloud Platform packages

    * `sudo pip install apache-beam`

    * `sudo pip sudo pip install apache-beam[gcp]`

* Test with the wordcount script

    * `python -m apache_beam.examples.wordcount --output counts.txt`

* Login to a cloud.google.com account, and enable Dataflow API on your project, using Google APIs

    * https://console.developers.google.com/apis/

* Use this bash script to test wordcount on your google cloud storage bucket

```
#/bin/bash
PROJECT='probable-byway-215618'
BUCKET='gs://lwheerman_umg_dataengr_denormalized_output’

python -m apache_beam.examples.wordcount \
  --project $PROJECT \
  --job_name $PROJECT-wordcount \
  --runner DataflowRunner \
  --staging_location $BUCKET/staging \
  --temp_location $BUCKET/temp \
  --output $BUCKET/output
```

* Make data in google cloud buckets publicly accessible

    * Bucket navigator -> find file -> … -> Edit Permissions -> Add + -> Entity:User, Name:allUsers, Access:Reader

    * https://cloud.google.com/storage/docs/access-control/making-data-public


## 3. Create your own pipeline

* Download wordcount.py form Apache Beam's github

    * https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount.py

* Use wordcount as your basis for your own pipeline


## 4. Apache Beam Snippets (Python)

### Python virtual-env
```
virtualenv umgtechnical-env/  # Create a virtual-env

. umgtechnical-env/bin/activate  # Activate a virtual-env

deactivate  # Deactivate a virtual-env
```

### Run pipeline locally
```
python $PYSCRIPT \
  --runner DirectRunner \
  --input_streams $DATAIN_STREAMS \
  --input_tracks $DATAIN_TRACKS \
  --input_users $DATAIN_USERS \
  --output streams_denorm.json
```

### Run pipeline on Google Cloud
```
python $PYSCRIPT \
  --runner DataflowRunner \
  --project $PROJECT \
  --job_name spotifystreamsdenorm \
  --staging_location $BUCKET_OUT/staging \
  --temp_location $BUCKET_OUT/temp \
  --input_streams $DATAIN_STREAMS \
  --input_tracks $DATAIN_TRACKS \
  --input_users $DATAIN_USERS \
  --output $BUCKET_OUT/streams_denorm_python.json
```

### Read from a file
```
tracks = (p
  | 'tracks_import' >> ReadFromText('data/users.txt'))

tracks = (p
  | 'tracks_import' >> ReadFromText('gs://gcp_project_name/gs_bucket/users.txt'))

streams = (p
  | 'streams_import' >> ReadFromText(file_pattern=known_args.users, 
                                     compression_type=CompressionTypes.GZIP))
```

### Write to a file
```
# Write output
final_output | WriteToText(known_args.output)
```

### Map Trasforms - Lambda or Passing in a Function
```
streams_by_track = (p
  | 'reading_streams' >> ReadFromText('datasets/streams_trunc')
  | 'from_json_streams' >> beam.Map(json.loads)
  | 'del_fields_streams' >> beam.Map(key_whitelist, stream_keys)
  | 'streams_track_map' >> beam.Map(lambda x:(x['track_id'], x)))
```

### Custom DoFn, for a ParDo
```
# Pull a field's value from JSON, and return a map of value-to-JSON
class MapFieldValueToJson(beam.DoFn):

  def __init__(self):
    super(MapFieldValueToJson, self).__init__()

  def process(self, json_text, map_key):
    try:
      fields = json.loads(json_text)
    except ValueError:
      fields = {} # drop corrupt JSON

    if map_key in fields:
        yield (fields[map_key], json_text)

...

map_jkey_input1 = (pc1
  | 'from_json' >> beam.ParDo(MapFieldValueToJson(), "user_id"))
```

### Custom PTransform
* Usually for compound transform (sequence of transforms)
* Also used for transforms that input 2 PCollections (ex: join)
* Transforms that output multiple PCollections are possible too
```
class JoinJsonDatasets(beam.PTransform):

  def __init__(self, join_key):
    super(JoinJsonDatasets, self).__init__()
    self.join_key = join_key

  def expand(self, keyedPcTuple):
    tags = keyedPcTuple.keys()
    if len(tags) < 2:
      return

    l1 = tags[0]
    l2 = tags[1]
    pc1 = keyedPcTuple[l1]
    pc2 = keyedPcTuple[l2]
```


**Pipeline Example**
```
# Map functions
def key_whitelist(element_map, whitelist):
  return {k: v for (k, v) in element_map.items() if k in whitelist}

# Begin dataflow pipeline
p = beam.Pipeline(options=pipeline_options)

# Prepare tracks dataset (map to track_id)
tracks_by_track = (p
  | 'tracks_import' >> ReadFromText(known_args.tracks)
  | 'tracks_from_json' >> beam.Map(json.loads)
  | 'tracks_key_whitelist' >> beam.Map(key_whitelist, TRACK_KEYS)
  | 'tracks_track_map' >> beam.Map(lambda x:(x['track_id'], x)))

# Prepare streams dataset (map to track_id)
streams_by_track = (p
  | 'streams_import' >> ReadFromText(known_args.streams)
  | 'streams_from_json' >> beam.Map(json.loads)
  | 'streams_key_whitelist' >> beam.Map(key_whitelist, STREAM_KEYS)
  | 'streams_track_map' >> beam.Map(lambda x:(x['track_id'], x)))

# Relational-Join: tracks, streams (cogroup)
tables = {'pc1': tracks_by_track, 'pc2': streams_by_track}
streams_track_joined = (tables
  | 'cogroup_tracks_streams' >> beam.CoGroupByKey()
  | 'combine_tracks_streams' >> beam.ParDo(CombineCoGroupResults()))
```


## References

* Setup Google Cloud

    * https://cloud.google.com/dataflow/docs/quickstarts/quickstart-python

    * https://cloud.google.com/billing/docs/how-to/modify-project

    * https://cloud.google.com/storage/docs/creating-buckets

    * https://beam.apache.org/documentation/runners/dataflow/

* Install Apache Beam for Python

    * https://beam.apache.org/get-started/quickstart-py/

* Google Cloud, Storage and Dataflow Runner

    * https://console.developers.google.com/apis/

    * https://cloud.google.com/storage/docs/access-control/making-data-public

* Beam Source

    * https://github.com/apache/beam/tree/master/sdks/python/apache_beam

    * https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples
