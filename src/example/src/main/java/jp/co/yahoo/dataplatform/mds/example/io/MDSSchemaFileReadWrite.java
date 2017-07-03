/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.dataplatform.mds.example.io;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import jp.co.yahoo.dataplatform.mds.schema.formatter.MDSSchemaStreamWriter;

import jp.co.yahoo.dataplatform.config.Configuration;

import jp.co.yahoo.dataplatform.schema.parser.JacksonMessageReader;
import jp.co.yahoo.dataplatform.schema.parser.IParser;
import jp.co.yahoo.dataplatform.mds.schema.parser.MDSSchemaReader;

public class MDSSchemaFileReadWrite {
  private static final String FILE_NAME = "/tmp/mds_file_fead_write_example.mds";

  private void cleanup(){
   File file = new File(FILE_NAME);
    if(file.exists()){
      System.out.println(String.format("delete old file::%s", FILE_NAME));
      file.delete();
    }
  }

  public void write() throws IOException{
    System.out.println("write message start");
    Path inputPath = Paths.get(MDSSchemaFileReadWrite.class.getClassLoader().getResource("sample_json.txt").getFile());
    OutputStream out = new FileOutputStream(FILE_NAME);
    MDSSchemaStreamWriter writer = new MDSSchemaStreamWriter(out, new Configuration());
    try(Stream<String> stream = Files.lines(inputPath)) {
      stream.forEach( line -> {
          try {
            System.out.println(String.format("message json::%s", line));
            byte[] message = line.getBytes();
            IParser parser = new JacksonMessageReader().create(message);
            System.out.println(String.join(",", parser.getAllKey()));
            writer.write(parser);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      );
      writer.close();
    }
  }

  public void read() throws IOException{
    InputStream in = new FileInputStream(FILE_NAME);
    MDSSchemaReader reader = new MDSSchemaReader();
    reader.setNewStream(in, new File(FILE_NAME).length(), new Configuration());
    IParser parser;
    System.out.println("read file start");
    while (reader.hasNext()){
      parser = reader.next();
      System.out.println(String.format("number = %d", parser.get("number").getInt()));
      System.out.println(String.format("summary = %s", String.join(", ", parser.getParser("summary").getAllKey())));
      System.out.println(String.format("summary.total_price = %d", parser.getParser("summary").get("total_price").getLong()));
    }
  }

  public int run() throws IOException {
    cleanup();
    write();
    read();
    return 0;
  }

  public static void main(final String[] args) throws IOException{
    new MDSSchemaFileReadWrite().run();
  }

}
