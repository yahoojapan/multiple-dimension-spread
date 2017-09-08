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
import java.io.InputStream;

import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import jp.co.yahoo.dataplatform.mds.MDSReader;
import jp.co.yahoo.dataplatform.mds.spread.Spread;

public class MDSCombineSpreadReader extends RecordReader<NullWritable, Spread> {

  private final MDSSpreadReader innerReader;

  public MDSCombineSpreadReader( final CombineFileSplit split , final TaskAttemptContext context , final Integer index ) throws IOException{
    Configuration config = context.getConfiguration();
    Path path = split.getPath( index );
    FileSystem fs = path.getFileSystem( config );
    long fileLength = fs.getLength( path );
    InputStream in = fs.open( path );

    innerReader = new MDSSpreadReader();
    innerReader.setStream( in , fileLength , 0 , fileLength );
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return innerReader.getCurrentKey();
  }

  @Override
  public Spread getCurrentValue() throws IOException, InterruptedException {
    return innerReader.getCurrentValue();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return innerReader.nextKeyValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return innerReader.getProgress();
  }

  @Override
  public void initialize( final InputSplit inputSplit, final TaskAttemptContext context ) throws IOException, InterruptedException {
  }

  @Override
  public void close() throws IOException {
    innerReader.close();
  }
}
