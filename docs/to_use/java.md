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
# Use of Multiple-Dimension-Spread in Java

Explain how to use it in Java.

# Record writer 
The class of writing in the record is [MDSRecordWriter](../../src/common/src/main/java/jp/co/yahoo/dataplatform/mds/MDSRecordWriter.java).

In this section, we will explain using implementation examples.
[Source code example](../../src/example/src/main/java/jp/co/yahoo/dataplatform/mds/example/io/MakeMDSFileStep1.java)

```
    in = this.getClass().getClassLoader().getResource( "sample_json.txt" ).openStream();
    br = new BufferedReader( new InputStreamReader( in ) );
    JacksonMessageReader jacksonReader = new JacksonMessageReader();
```
Opening a JSON file and setting up a JSON parser.

```
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    MDSRecordWriter writer = new MDSRecordWriter( out , new Configuration() );
```
Create an OutputStream to write to and set up MDSRecordWriter.
In this example, the setting is not specially set as default.

```
    while( br.ready() ){
      writer.addParserRow( jacksonReader.create( br.readLine() ) );
    }
```
JacksonMessageReader converts JSON message to IParser and add it to MDSRecordWriter.

```
    writer.close();
```
Be sure to close.
If you do not close it, any data not converted in memory will be lost.

# Record reader
The class of reading in the record is [MDSSchemaReader](../../src/schema/src/main/java/jp/co/yahoo/dataplatform/mds/schema/parser/MDSSchemaReader.java).

In this section, we will explain using implementation examples.
[Source code example](../../src/example/src/main/java/jp/co/yahoo/dataplatform/mds/example/io/MDSSchemaFileReadWrite.java)

```
    InputStream in = new FileInputStream(FILE_NAME);
    MDSSchemaReader reader = new MDSSchemaReader();
    reader.setNewStream(in, new File(FILE_NAME).length(), new Configuration());
```
Open the file and set OutputStream to MDSSchemaReader.
In this example, the setting is not changed, so it defaults.

```
    IParser parser;
    System.out.println("read file start");
    while (reader.hasNext()){
      parser = reader.next();
      System.out.println(String.format("number = %d", parser.get("number").getInt()));
      System.out.println(String.format("summary = %s", String.join(", ", parser.getParser("summary").getAllKey())));
      System.out.println(String.format("summary.total_price = %d", parser.getParser("summary").get("total_price").getLong()));
    }
```
Process all records contained in the file.
In this Reader, it represents IParser which represents records.

# Configuration
Describe the MDS settings.

| name | description |
|:-----|:------------|
| block.size | MDS block byte size. |
| block.maker.compress.class | Compression class used for block meta. Default is uncompressed. |
| spread.column.maker.default.compress.class | Column compression class. The default is gzip. |
| spread.column.maker.setting | Set the encoding class of the column. When this function is turned on, it is not automatically selected, and conversion is performed with this setting. |
| spread.column.maker.use.auto.optimizer | Automatically select the encoding of the column. Because we take statistical information to make the selection, the encoding time increases. The default is true. |
| spread.column.maker.use.auto.optimizer.factory.class | Specify the class for selecting the encoding. |
| spread.reader.read.column.names | Setting of column to be read. |
| spread.reader.expand.column | Specify column to expand Array. |
| spread.reader.flatten.column | Specify the column to flatten the Array. |
| spread.size | In RecordWriter, the Max Spread size. |
| record.writer.max.rows | In RecordWriter, the Max record size. |
