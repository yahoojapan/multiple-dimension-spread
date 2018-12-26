<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
# Command line tool

# Preparation
CLI is a Command Line Interface tool for using MDS.
following tools are provided.

* bin/setup.sh # for gathering MDS related jars
* bin/mds.sh   # create mds data, and show data

mds.sh needs some jars, so please create jar files before using.

    $ mvn package

For preparation, get MDS jars and store then to proper directories.

    $ bin/setup.sh # get MDS jars from Maven repository (bin/setup.sh -h for help)

# mds.sh

## help
Output usage.


* Example
```
$ ./bin/mds.sh help
setup  setup mds lib dir.
create create file.
cat read mds file.
schema view mds file schema.
fstats view mds file stats.
cstats view column stats.
stest run storage perfomance test.
help view help. 
```

## create

Create an MDS file.

| args | Required | detail |
|:-----|:--------:|:-------|
| -f,--format <format> |true|Input data format. Now only json is supported.|
|-h , --help | false | Output usage.|
| -i,--input <input> | true | Input file path.  "-" is standard input. |
| -o,--output <output> | true | Output file path. "-" is standard output |
| -s,--schema <schema> | false | If need a schema with input data format please enter it.|


Example of execution
```
$ bin/mds.sh create -i src/example/src/main/resources/sample_json.txt -f json -o /tmp/sample.mds
```

## cat 
Commands for reading and outputting MDS files.

| args | Required | detail |
|:-----|:--------:|:-------|
| -e,--expand <expand> | false | Use expand function. |
| -f,--format <format> | true |  Output data format. Supports only output with json now |
| -h,--help | false | Output usage. |
| -i,--input <input> | true | Input file path. Input file path.  "-" is standard input.|
| -o,--output <output> | true | Output file path. "-" is standard output |
| -p,--projection_pushdown <projection_pushdown> | false | Use projection pushdown. Format:"[ [ "column1" , "[column1-child]" , "column1-child-child" ] [ "column2" , ... ] ... ]" |
| -s,--schema <schema> | false | If need a schema with output data format please enter it.  |
| -x,--flatten <flatten> | false | Use flatten function. |

Example of execution
```
$ bin/mds.sh cat -i /tmp/sample.mds -o '-' # show whole data
{"summary":{"total_price":550,"total_weight":412},"number":5,"price":110,"name":"apple","class":"fruits"}
{"summary":{"total_price":800,"total_weight":600},"number":10,"price":80,"name":"orange","class":"fruits"}

$ bin/mds.sh cat -i /tmp/sample.mds -o '-' -p '[["name"]]' # show part of data
{"name":"apple"}
{"name":"orange"}
```

## schema
This command outputs the schema of the MDS file.

| args | Required | detail |
|:-----|:--------:|:-------|
| -f,--format <format> | true | Output data format. Supports only output with hive now. |
| -h,--help | false | Output usage. |
| -i,--input <input> | true | Input file path. Input file path.  "-" is standard input. |
| -o,--output <output> | true | Output file path. "-" is standard output |

Example of execution
```
$ bin/mds.sh schema -i /tmp/sample.mds -o '-' -f hive
struct<summary:struct<total_price:int,total_weight:int>,number:int,price:int,name:string,class:string>
```

## fstats
Output statistical information of the file.

| args | Required | detail |
|:-----|:--------:|:-------|
| -h,--help | false | Output usage. |
| -i,--input <input> | true | Input file path. Input file path.  "-" is standard input. |
| -o,--output <output> | true | Output file path. "-" is standard output |

Example of execution
```
$ bin/mds.sh fstats -i /tmp/sample.mds  -o "-"
Line count=2 , Average record size=113.500000 , Average record real size=139.500000 , Average record per field=7.000000 , Field count=14 , Raw data size=227 , Real data size=279 , Logical data size=75 , cardinality=-1 , stats report count=1 , Average field size=16.214286 , Average field real size=19.928571 , Compress late=1.229075 , Average row count per stats report count=14.000000 , Average cardinality=-1.000000
```

## cstats
Output statistical information of the column.

| args | Required | detail |
|:-----|:--------:|:-------|
| -h,--help | false | Output usage. |
| -i,--input <input> | true | Input file path. Input file path.  "-" is standard input. |
| -o,--output <output> | true | Output file path. "-" is standard output |

Example of execution
```
$  bin/mds.sh cstats -i /tmp/sample.mds  -o "-"
/ROOT/summary<SPREAD> : Field count=2 , Raw data size=0 , Real data size=0 , Logical data size=0 , cardinality=-1 , stats report count=1 , Average field size=0.000000 , Average field real size=0.000000 , Compress late=NaN , Average row count per stats report count=2.000000 , Average cardinality=-1.000000
/ROOT/summary/total_price<INTEGER> : Field count=2 , Raw data size=48 , Real data size=48 , Logical data size=8 , cardinality=-1 , stats report count=1 , Average field size=24.000000 , Average field real size=24.000000 , Compress late=1.000000 , Average row count per stats report count=2.000000 , Average cardinality=-1.000000
/ROOT/summary/total_weight<INTEGER> : Field count=2 , Raw data size=48 , Real data size=48 , Logical data size=8 , cardinality=-1 , stats report count=1 , Average field size=24.000000 , Average field real size=24.000000 , Compress late=1.000000 , Average row count per stats report count=2.000000 , Average cardinality=-1.000000
/ROOT/number<INTEGER> : Field count=2 , Raw data size=48 , Real data size=48 , Logical data size=8 , cardinality=-1 , stats report count=1 , Average field size=24.000000 , Average field real size=24.000000 , Compress late=1.000000 , Average row count per stats report count=2.000000 , Average cardinality=-1.000000
/ROOT/price<INTEGER> : Field count=2 , Raw data size=48 , Real data size=48 , Logical data size=8 , cardinality=-1 , stats report count=1 , Average field size=24.000000 , Average field real size=24.000000 , Compress late=1.000000 , Average row count per stats report count=2.000000 , Average cardinality=-1.000000
/ROOT/name<STRING> : Field count=2 , Raw data size=25 , Real data size=77 , Logical data size=19 , cardinality=3 , stats report count=1 , Average field size=12.500000 , Average field real size=38.500000 , Compress late=3.080000 , Average row count per stats report count=2.000000 , Average cardinality=3.000000
/ROOT/class<STRING> : Field count=2 , Raw data size=10 , Real data size=10 , Logical data size=24 , cardinality=1 , stats report count=1 , Average field size=5.000000 , Average field real size=5.000000 , Compress late=1.000000 , Average row count per stats report count=2.000000 , Average cardinality=1.000000
```

## stest
Measure reading and writing performance.

| args | Required | detail |
|:-----|:--------:|:-------|
| -h,--help | false | Output usage. |
| -i,--input <input> | true | Input file path. Input file path.  "-" is standard input. |
| -n,--maxSpreadCount <spread_count> | false | Max spread count. |
| -p,--projection_pushdown <projection_pushdown> | false | Use projection pushdown. |
| -o,--output <output> | true | Output file path. "-" is standard output |

The output format is as follows.
```
<Column Path> , <Name of ColumnBinaryMaker> , <Name of ICompressor> , <Row count> , <Raw data size> , <Real data size> , <Logical data size> , <Write CPU time(msec)> , <Read CPU time(msec)>
```

Example of execution
```
$ bin/mds.sh stest -i /tmp/sample.mds  -o "-"
/summary/total_price,jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeDumpLongColumnBinaryMaker,jp.co.yahoo.dataplatform.mds.compressor.DefaultCompressor,2,22,22,4,1.089,1.548
/summary/total_price,jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeDumpLongColumnBinaryMaker,jp.co.yahoo.dataplatform.mds.compressor.GzipCompressor,2,46,46,4,0.778,0.189
/summary/total_price,jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeLongColumnBinaryMaker,jp.co.yahoo.dataplatform.mds.compressor.DefaultCompressor,2,24,24,4,1.134,4.039
/summary/total_price,jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeLongColumnBinaryMaker,jp.co.yahoo.dataplatform.mds.compressor.GzipCompressor,2,48,48,4,0.206,0.225
/summary/total_weight,jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeDumpLongColumnBinaryMaker,jp.co.yahoo.dataplatform.mds.compressor.DefaultCompressor,2,22,22,4,0.111,0.131
/summary/total_weight,jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeDumpLongColumnBinaryMaker,jp.co.yahoo.dataplatform.mds.compressor.GzipCompressor,2,46,46,4,0.190,0.171
/summary/total_weight,jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeLongColumnBinaryMaker,jp.co.yahoo.dataplatform.mds.compressor.DefaultCompressor,2,24,24,4,0.164,0.165
/summary/total_weight,jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeLongColumnBinaryMaker,jp.co.yahoo.dataplatform.mds.compressor.GzipCompressor,2,48,48,4,0.186,0.259
/number,jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeDumpLongColumnBinaryMaker,jp.co.yahoo.dataplatform.mds.compressor.DefaultCompressor,2,20,20,2,0.624,0.112
/number,jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeDumpLongColumnBinaryMaker,jp.co.yahoo.dataplatform.mds.compressor.GzipCompressor,2,44,44,2,0.152,0.157
/number,jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeLongColumnBinaryMaker,jp.co.yahoo.dataplatform.mds.compressor.DefaultCompressor,2,21,21,2,0.652,0.102
/number,jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeLongColumnBinaryMaker,jp.co.yahoo.dataplatform.mds.compressor.GzipCompressor,2,45,45,2,0.180,0.153
```

## merge
Merging MDS files.

| args | Required | detail |
|:-----|:--------:|:-------|
| -h,--help | false | Output usage. |
| -i,--input <input> | true | Input file path. Input file path.  "-" is standard input. |
| -o,--output <output> | true | Output file path. "-" is standard output |
| -p,--projection_pushdown <projection_pushdown> | false | Use projection pushdown. |
| -x,--flatten <flatten> | false | Use flatten function. |

Example of execution
```
$ bin/mds.sh merge -i "/tmp/sample.mds,/tmp/sample.mds"  -o "/tmp/merge_sample.mds"
```

## to_arrow
Commands for creating Apache Arrow files from MDS files.

| args | Required | detail |
|:-----|:--------:|:-------|
| -e,--expand <expand> | false | Use expand function. |
| -h,--help | false | Output usage. |
| -i,--input <input> | true | Input file path. Input file path.  "-" is standard input. |
| -o,--output <output> | true | Output file path. "-" is standard output |
| -p,--projection_pushdown <projection_pushdown> | false | Use projection pushdown. |
| -x,--flatten <flatten> | false | Use flatten function. |

Example of execution
```
$ ./bin/mds.sh to_arrow -i /tmp/sample.mds -o "/tmp/sample.arrow"
```

Examples of using data in [pyarrow(https://arrow.apache.org/docs/python/)]

- sample.py
```
import pyarrow as pa

reader = pa.RecordBatchFileReader( pa.OSFile( "/tmp/sample.arrow" ) )

for i in range( reader.num_record_batches ):
  rb = reader.get_record_batch(i)
  print( rb.num_rows )
  df = rb.to_pandas()
  print( df["name"].value_counts() )
```

Runnning command
```
$ python sample.py
2
apple     1
orange    1
Name: name, dtype: int64
```
