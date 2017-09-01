# MDS(Multiple Dimesion Spread)
MDS is a Schema-less columnar storage format.

Provide flexible representation like JSON and efficient reading similar to other columnar storage formats.

## Requirements

* Mac OS X or Linux
* Java 8 Update 92 or higher (8u92+), 64-bit
* Maven 3.3.9+ (for building)
* Hive 2.0 or later

## Preparement
* Install gpg and create a gpg key for maven plugin.
```
gpg --gen-key
gpg --list-keys
```
* Make sure you had installed the following two other projects before.
https://github.com/yahoojapan/dataplatform-schema-lib
https://github.com/yahoojapan/dataplatform-config


## Building MDS Package
MDS is a standard Maven project. Simply run the following command from the project root directory:

    mvn clean install

On the first build, Maven will download all the dependencies from the internet and cache them in the local repository (`~/.m2/repository`), which can take a considerable amount of time. Subsequent builds will be faster.

MDS has a comprehensive set of unit tests that can take several minutes to run. You can disable the tests when building:

    mvn clean install -DskipTests

## Getting Started
* [Java](docs/getting_started_java.md)
* [Hadoop InputFormat](docs/getting_started_hadoop.md)
* [Hive](docs/getting_started_hive.md)
* [Arrow](docs/getting_started_arrow.md)
