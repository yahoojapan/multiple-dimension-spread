## Requirements
Hive 2.0.X

## Support data type

### Numeric Types
| Type       | Supported    |
|:-----------|:------------:|
|TINYINT|**true**|
|SMALLINT|**true**|
|INTEGER|**true**|
|FLOAT|**true**|
|DOUBLE|**true**|
|DECIMAL|**false**|

### Date/Time Types
| Type       | Supported    |
|:-----------|:------------:|
|TIMESTAMP|**false**|
|DATE|**false**|
|INTERVAL|**false**|

### String Types
| Type       | Supported    |
|:-----------|:------------:|
|STRING|**true**|
|VARCHAR|**false**|
|CHAR|**false**|

### Misc Types
| Type       | Supported    |
|:-----------|:------------:|
|BOOLEAN|**true**|
|BINARY|**true**|

### Complex Types
| Type       | Supported    |
|:-----------|:------------:|
|ARRAYS|**true**|
|MAPS|**true**|
|STRUCT|**true**|
|UNION|**true**|

## jars
- yjava_jp_dataplatform_config.jar
- yjava_jp_dataplatform_schema_common.jar
- yjava_jp_dataplatform_schema_hive.jar
- yjava_jp_dataplatform_schema_jackson.jar
- yjava_dataplatform_mds_common.jar
- yjava_dataplatform_mds_hive.jar
- yjava_dataplatform_mds_schema.jar

## Setup Hive
See [Apache Hive](https://hive.apache.org/)

## Add jars
```
add jar yjava_jp_dataplatform_config.jar;
add jar yjava_jp_dataplatform_schema_common.jar;
add jar yjava_jp_dataplatform_schema_hive.jar;
add jar yjava_jp_dataplatform_schema_jackson.jar;
add jar yjava_dataplatform_mds_common.jar;
add jar yjava_dataplatform_mds_hive.jar;
add jar yjava_dataplatform_mds_schema.jar;
```

## Create table
```
create table sample(
  name string,
  price bigint
)
ROW FORMAT SERDE
  'jp.co.yahoo.dataplatform.mds.hadoop.hive.MDSSerde'
STORED AS INPUTFORMAT
  'jp.co.yahoo.dataplatform.mds.hadoop.hive.io.MDSHiveLineInputFormat'
OUTPUTFORMAT
  'jp.co.yahoo.dataplatform.mds.hadoop.hive.io.MDSHiveParserOutputFormat'
```
## Make complicated data correspond to vectorization
Supports array expansion

![ArrayExpand](images/expand.png)

Supports flattening Map and Struct types

![Flatten](images/flatten.png)

### Expand table
This feature is read only.

### Flatten table
This feature is read only.

### Expand and Flatten table
This feature is read only.

