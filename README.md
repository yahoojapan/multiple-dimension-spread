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

# MDS (acronym of Multiple Dimension Spread)

# Introduction
## What does this project do?
MDS (acronym of Multiple Dimension Spread) is a Schema-less columnar storage format.
Provide flexible representation like JSON and efficient reading
similar to other columnar storage formats.


## Why is this project useful?
There was a problem that it is too large to compress
and save the data as it is in the Big Data era.
From the demand for improvement in compression ratio and read performance,
several columnar data formats (for example, Apache ORC and Apache Parquet)
were proposed.
They achieve the high compression ratio from similar data in column
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
And stored data can be used with defining its schema at the time of analyzing.
See [DataLake](https://en.wikipedia.org/wiki/Data_lake).


## How do I get started?
Firstly, please get MDS related repositories following section named "How to get source".

MDS format can treat data without Hadoop environment.
However, it is useful for big data.
so, it needs a Hadoop environment for storage and Hive for read to use efficiently.

We have a plan to create a docker environment of Hadoop and Hive for test use,
but current situation, you need to prepare Hadoop and Hive firstly.

### Setup environment
- [Apache Hadoop](https://hadoop.apache.org)
- [Apache Hive](https://hive.apache.org/)


## CLI
CLI is a Command Line Interface tool for using MDS.
following tools are provided.

* bin/setup.sh # for gathering MDS related jars
* bin/mds.sh   # create mds data, and show data

mds.sh needs some jars, so please create jar files before using.

    $ mvn package

## How to use
### Preparation
For preparation, get MDS jars and store then to proper directories.

    $ bin/setup.sh # get MDS jars from Maven repository (bin/setup.sh -h for help)

And, put MDS related jars to Hadoop.

    $ cp -r jars/mds /tmp/mds_lib
    $ hdfs dfs -put -r /tmp/mds_lib /mds_lib

### Create MDS formatted file
convert JSON data to MDS format.

    $ bin/mds.sh create -i src/example/src/main/resources/sample_json.txt -f json -o /tmp/sample.mds
    $ bin/mds.sh cat -i /tmp/sample.mds -o '-' # show whole data
    {"summary":{"total_price":550,"total_weight":412},"number":5,"price":110,"name":"apple","class":"fruits"}
    {"summary":{"total_price":800,"total_weight":600},"number":10,"price":80,"name":"orange","class":"fruits"}
    $ bin/mds.sh cat -i /tmp/sample.mds -o '-' -p '[ ["name"] ]' # show part of data
    {"name":"apple"}
    {"name":"orange"}

### Copy MDS file to HDFS environment
Copy MDS file to HDFS environment.

    $ hdfs dfs -mkdir -p /tmp/ss
    $ hdfs dfs -put /tmp/sample.mds /tmp/ss/sample.mds

### Read MDS file using Hive
Enter Hive and add jar files to use MDS format.

    $ hive -i jars/mds/add_jar.hql
    > create database test;
    > use test;
    > create external table sample_json (
        summary struct<total_price: bigint, total_weight: bigint>,
        number bigint,
        price bigint,
        name string,
        class string
      )
      ROW FORMAT SERDE
        'jp.co.yahoo.dataplatform.mds.hadoop.hive.MDSSerde'
      STORED AS INPUTFORMAT
        'jp.co.yahoo.dataplatform.mds.hadoop.hive.io.MDSHiveLineInputFormat'
      OUTPUTFORMAT
        'jp.co.yahoo.dataplatform.mds.hadoop.hive.io.MDSHiveParserOutputFormat'
      location '/tmp/ss';
    > select * from sample_json;
    {"total_price":550,"total_weight":412}  5 110 apple fruits
    {"total_price":800,"total_weight":600}  10  80  orange  fruits

See [document Hive](docs/getting_started_hive.md) for further detail to use.


## Where can I get more help, if I need it?
Support and discussion of MDS are on the Mailing list.
Please refer the following subsection named "How to contribute".

We plan to support and discussion of MDS on the Mailing list.
However, please contact us via GitHub until ML is opened.

# How to contribute
We welcome to join this project widely.

## Document
See [document MDS](docs/develop/developing_mds.md)

## License
This project is on the [Apache License](https://www.apache.org/licenses/LICENSE-2.0).
Please treat this project under this license.

## Mailing list
User support and discussion of MDS development are on the following Mailing list.
Please send a blank e-mail to the following address.

* subscribe: open_mds+subscribe@googlegroups.com
* unsubscribe: open_mds+unsubscribe@googlegroups.com

[Archive](https://groups.google.com/forum/#!forum/open_mds) is useful for what was communicated at this project.

## for Developer
Please accept [Contributer licence agreement](https://gist.github.com/ydnjp/3095832f100d5c3d2592)
when participating as a developer.

We invite you to [JIRA](https://multiple-dimension-spread.atlassian.net) as a bug tracking,
when you mentioned in the above Mailing list.


## System requirement
Following environments are required.

* Mac OS X or Linux
* Java 8 Update 92 or higher (8u92+), 64-bit
* Maven 3.3.9 or later (for building)
* Hadoop 2.7.3 or later
* Hive 2.0 or later (for reading data)


# How to get the source
MDS library constructs jar files on following modules.

* multiple-dimension-spread
* dataplatform-config
* dataplatform-schema-lib


## GitHub
MDS sources are there.

* [multiple-dimension-spread](https://github.com/yahoojapan/multiple-dimension-spread.git)
* [dataplatform-config](https://github.com/yahoojapan/dataplatform-schema-lib.git)
* [dataplatform-schema-lib](https://github.com/yahoojapan/dataplatform-config.git)

### Preparement
Install gpg and create a gpg key for maven plugin to use git clone.

    gpg --gen-key
    gpg --list-keys

Add following gpg setting to maven-local-repository-home/conf/settings.xml .
Usually, maven-local-repository-home is $HOME/.m2 .

    </profiles>
      <profile>
        <id>sign</id>
        <activation>
            <activeByDefault>true</activeByDefault>
        </activation>
        <properties>
            <gpg.passphrase>***YOUR-PASSPHRASE***</gpg.passphrase>
        </properties>
      </profile>
    </profiles>

## Maven
MDS sources can get from the Maven repository.

### multiple-dimension-spread

* [multiple-dimension-spread-arrow](https://mvnrepository.com/artifact/jp.co.yahoo.dataplatform.mds/multiple-dimension-spread-arrow)
* [multiple-dimension-spread-common](https://mvnrepository.com/artifact/jp.co.yahoo.dataplatform.mds/multiple-dimension-spread-common)
* [multiple-dimension-spread-hadoop](https://mvnrepository.com/artifact/jp.co.yahoo.dataplatform.mds/multiple-dimension-spread-hadoop)
* [multiple-dimension-spread-hive](https://mvnrepository.com/artifact/jp.co.yahoo.dataplatform.mds/multiple-dimension-spread-hive)
* [multiple-dimension-spread-schema](https://mvnrepository.com/artifact/jp.co.yahoo.dataplatform.mds/multiple-dimension-spread-schema)


### dataplatform-config
* [dataplatform-common-config](https://mvnrepository.com/artifact/jp.co.yahoo.dataplatform.config)

### dataplatform-schema-lib

* [schema-common](https://mvnrepository.com/artifact/jp.co.yahoo.dataplatform.schema/schema-common)
* [schema-hive](https://mvnrepository.com/artifact/jp.co.yahoo.dataplatform.schema/schema-hive)
* [schema-jackson](https://mvnrepository.com/artifact/jp.co.yahoo.dataplatform.schema/schema-jackson)
* [schema-orc](https://mvnrepository.com/artifact/jp.co.yahoo.dataplatform.schema/schema-orc)


## Compile sources
Compile each source following instructions.

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

# Next Reading
* [How to use Java](docs/to_use/java.md)
* [How to use Hive](docs/getting_started_hive.md)

# MISC
## Change Logs
## FAQ

