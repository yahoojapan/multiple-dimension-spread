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
package jp.co.yahoo.dataplatform.mds.hadoop.mapreduce;

import java.io.IOException;
import java.io.OutputStream;

import jp.co.yahoo.dataplatform.mds.MDSRecordWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import jp.co.yahoo.dataplatform.schema.parser.IParser;

import jp.co.yahoo.dataplatform.config.Configuration;

public class MDSParserRecordWriter extends RecordWriter<NullWritable, IParser> {

  private final MDSRecordWriter writer;

  public MDSParserRecordWriter( final OutputStream out , final Configuration config ) throws IOException{
    writer = new MDSRecordWriter( out , config );
  }

  @Override
  public void close( final TaskAttemptContext context ) throws IOException, InterruptedException {
    writer.close();
  }

  @Override
  public void write( final NullWritable key , final IParser value ) throws IOException, InterruptedException {
    writer.addParserRow( value );
  }

}
