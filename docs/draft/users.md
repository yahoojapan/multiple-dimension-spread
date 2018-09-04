# Introducton
## What does this project do?
MDS (acronym of Multiple Dimension Spread) is a Schema-less columnar storage format.
Provide flexible representation like JSON and efficient reading
similar to other columnar storage formats.


## Why is this project useful?
There was a problem that it is too large to compress
and save the data as it is in the Big Data era.
From the demand of improvement in compression ratio and read performance,
several columnar data formats (for example, Apache ORC and Apache Parquet)
were proposed.
They achived the high compression ratio from similar data in column
and reading performance for grouping data by column when data is used.

However, these data formats are required
the data structure in a row (or a record) should be defined
before saving the data.
It was necessary to decide how to use it at the time of data storage,
and it was often a problem that it was difficult to decide
what kind of data to use.

In this project, we provide a new columnar format
which does not require the schema at the time of data storage
with compression and read performance equal to (or higher in case)
than other formats.


## Use cases
### Data Analysis
Analyzing big data requires store data compactly and get data smoothly.
MDS as a columnar format is useful for this needs.

### Data Lake
Data Lake is a data pool that is not required the data structure
(as a schema) in the row at the time of data storage. 
And storaged data can be used with defining its schema at the time of analyze.
See [DataLake][https://en.wikipedia.org/wiki/Data_lake].


## How do I get started?
### Docker
If you have docker environment (or can install it),
you can use our library easily as a test.
Following commands are for get docker image and enter the environment.

    $ docker pull multiple-dimension-spread # get image
    $ docker run -it multiple-dimension-spread bash # run and enter container

This environment provides hadoop, hive and CLI.
Then, you can use through CLI described later.


### Get manually
If you have hadoop environment and hive already (or can install them),
MDS CLI tool and jar files for using Hive can be gotten from source or Maven repository.
So, please refer following section named "How to get source".


### CLI
CLI is a Command Line Interface tool for using MDS.

For preparation, get MDS jars and store proper directories.

    $ tools/mds jars get /local/mds/jars # get jars from Maven Repository
    $ tools/mds jars put /local/mds/jars /hdfs/mds/jars
    $ tools/mds create hql add_jars.hql /local/mds/jars /hdfs/mds/jars

And then, convert json data to MDS format and store in the hadoop,
and read it using hive.

    $ tools/mds store data.json /hadoop/data.mds
    $ hive -i add_jars.hql # enter hive and add jar files
    > xxxx

## Where can I get more help, if I need it?
Support and discussion of MDS are on the Mailing list.
Please refer following subsection named "Contribute".


# How to contribute
We welcome to join this project widely.


## License
This project is on the [Apache License][https://www.apache.org/licenses/LICENSE-2.0].
Please treat this project under this license.


## Mailing list
User support and discussion of MDS development are
on the following Mailing list.

* subscribe: open\_mds+subscribe@googlegroups.com
* unsubscribe: open\_mds+unsubscribe@googlegroups.com

[Archive][https://groups.google.com/forum/#!forum/open_mds] is
useful for what was communicated at this project.


## Bug tracker
We have ticket system for bug tracking and handle developing status.
Please sign up [Bug tracking][], and post the bug.


## System requirement
Following environments are required.

* Hadoop ${version}
* Hive 2.0 or later (for reading data)
* Java 8 Update 92 or higher (8u92+), 64-bit
* Maven 3.3.9+ (for building)


## System overview
This data format is to store data on Hadoop,
and read the data using MDS jars.
So, use this under follwing steps.

1. data source
2. store data
3. Hadoop
4. Hive (using MDS jars) to use stored data


# How to get source
MDS library constructs jar files on following modules.

* multiple-dimension-spread
* dataplatform-config
* dataplatform-schema-lib

## GitHub
MDS sources are there.

* [multiple-dimension-spread][https://github.com/yahoojapan/multiple-dimension-spread.git]
* [dataplatform-config][https://github.com/yahoojapan/dataplatform-schema-lib.git]
* [dataplatform-schema-lib][https://github.com/yahoojapan/dataplatform-config.git]


## Maven
MDS sources can get from Maven repository.

### multiple-dimension-spread

* [multiple-dimension-spread-arrow][https://mvnrepository.com/artifact/jp.co.yahoo.dataplatform.mds/multiple-dimension-spread-arrow]
* [multiple-dimension-spread-common][https://mvnrepository.com/artifact/jp.co.yahoo.dataplatform.mds/multiple-dimension-spread-common]
* [multiple-dimension-spread-hadoop][https://mvnrepository.com/artifact/jp.co.yahoo.dataplatform.mds/multiple-dimension-spread-hadoop]
* [multiple-dimension-spread-hive][https://mvnrepository.com/artifact/jp.co.yahoo.dataplatform.mds/multiple-dimension-spread-hive]
* [multiple-dimension-spread-schema][https://mvnrepository.com/artifact/jp.co.yahoo.dataplatform.mds/multiple-dimension-spread-schema]


### dataplatform-schema-lib

* [schema-common][https://mvnrepository.com/artifact/jp.co.yahoo.dataplatform.schema/schema-common]
* [schema-hive][https://mvnrepository.com/artifact/jp.co.yahoo.dataplatform.schema/schema-hive]
* [schema-jackson][https://mvnrepository.com/artifact/jp.co.yahoo.dataplatform.schema/schema-jackson]
* [schema-orc][https://mvnrepository.com/artifact/jp.co.yahoo.dataplatform.schema/schema-orc]


## Compile sources
Compile each source following instruction.

### multiple-dimension-spread

    $ cd /local/mds/home
    $ git clone https://github.com/yahoojapan/multiple-dimension-spread.git
    $ cd multiple-dimension-spread
    $ mvn clean install
    
### dataplatform-schema-lib

    $ cd /local/mds/home
    $ git clone https://github.com/yahoojapan/dataplatform-schema-lib.git
    $ cd dataplatform-schema-lib
    $ mvn clean install
    
### dataplatform-config

    $ cd /local/mds/home
    $ git clone https://github.com/yahoojapan/dataplatform-config.git
    $ cd dataplatform-config
    $ mvn clean install


# Usage
CLI tool is useful for preparation and use MDS library.

## Preparation
For preparation, get MDS jars and store proper directories.

### Get jar files
Gather the jar files from local directory after compiling sources.

    $ tools/mds jars get /local/mds/jars /local/mds/home

Or, from Maven repository.

    $ tools/mds jars get /local/mds/jars # get jars from Maven Repository

### Store jar files to hdfs

    $ tools/mds jars put /local/mds/jars /hdfs/mds/jars

### Create add\_jar hql file

    $ tools/mds create hql add_jars.hql /local/mds/jars /hdfs/mds/jars


## Use MDS
And then, convert json data to MDS format and store in the hadoop,
and read it using hive.

### Store data

    $ tools/mds store data.json /hadoop/data.mds

### Read Data

    $ hive -i add_jars.hql # enter hive and add jar files
    > xxxx


# MISC
## Change Logs
## FAQ

