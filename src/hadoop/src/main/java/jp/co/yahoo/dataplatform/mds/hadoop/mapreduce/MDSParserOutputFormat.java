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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import jp.co.yahoo.dataplatform.schema.parser.IParser;

public class MDSParserOutputFormat extends FileOutputFormat<NullWritable,IParser>{

  @Override
  public RecordWriter<NullWritable,IParser> getRecordWriter( final TaskAttemptContext taskAttemptContext ) throws IOException,InterruptedException{
    Configuration config = taskAttemptContext.getConfiguration(); 

    String extension = ".mds";
    Path file = getDefaultWorkFile( taskAttemptContext, extension );

    FileSystem fs = file.getFileSystem( config );
    long dfsBlockSize = Math.max( fs.getDefaultBlockSize( file ), 1024 * 1024 * 32 );

    OutputStream out = fs.create( file , true , 4096 , fs.getDefaultReplication(file) , dfsBlockSize );

    jp.co.yahoo.dataplatform.config.Configuration writerConfig = new jp.co.yahoo.dataplatform.config.Configuration();
    writerConfig.set( "spread.size" , Integer.toString( 1024 * 1024 * 64 ) );
    return new MDSParserRecordWriter( out , writerConfig );
  }


}
