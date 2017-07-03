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
package jp.co.yahoo.dataplatform.mds.hadoop.hive.io;

import java.io.IOException;
import java.io.OutputStream;

import jp.co.yahoo.dataplatform.mds.MDSRecordWriter;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;

import jp.co.yahoo.dataplatform.config.Configuration;

public class MDSHiveRecordWriter implements RecordWriter {

  private final MDSRecordWriter writer;

  public MDSHiveRecordWriter( final OutputStream out , final Configuration config ) throws IOException{
    writer = new MDSRecordWriter( out , config );
  }

  @Override
  public void close( final boolean abort ) throws IOException {
    writer.close();
  }

  @Override
  public void write( final Writable writable ) throws IOException {
    writer.addParserRow( ( (ParserWritable)writable ).parser );
  }

}
