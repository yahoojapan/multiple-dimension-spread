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
package jp.co.yahoo.dataplatform.mds;

import java.io.IOException;
import java.io.OutputStream;

import java.util.Map;

import jp.co.yahoo.dataplatform.config.Configuration;

import jp.co.yahoo.dataplatform.schema.parser.IParser;

import jp.co.yahoo.dataplatform.mds.spread.Spread;

public class MDSRecordWriter implements AutoCloseable{

  public static final int DEFAULT_SPREAD_SIZE = 1024 * 1024 * 224;
  public static final int DEFAULT_MAX_RECORDS = Short.MAX_VALUE - 1;
  public static final int DEFAULT_MIN_SIZE = 1024 * 1024 * 16;
  public static final int DEFAULT_MIN_RECORDS = 1000;

  private final MDSWriter fileWriter;
  private int maxRows;
  private int currentDataSize;
  private int currentRows;
  private int spreadSize;
  
  private Spread currentSpread;

  public MDSRecordWriter( final OutputStream out ) throws IOException{
    this( out , new Configuration() );
  }

  public MDSRecordWriter( final OutputStream out , final Configuration config ) throws IOException{
    fileWriter = new MDSWriter( out , config );
    currentSpread = new Spread();
    spreadSize = config.getInt( "spread.size" , DEFAULT_SPREAD_SIZE );
    if( spreadSize < DEFAULT_MIN_SIZE ){
      spreadSize = DEFAULT_MIN_SIZE;
    }
    maxRows = config.getInt( "record.writer.max.rows" , DEFAULT_MAX_RECORDS );
    if( maxRows < DEFAULT_MIN_RECORDS ){
      maxRows = DEFAULT_MIN_RECORDS;
    }
  }

  public void addRow( final Map<String,Object> row ) throws IOException{
    currentDataSize += currentSpread.addRow( row );
    currentRows++;
    flushSpread();
  }

  public void addParserRow( final IParser parser )throws IOException{
    currentDataSize += currentSpread.addParserRow( parser );
    currentRows++;
    flushSpread();
  }

  private void flushSpread() throws IOException{
    if( spreadSize < currentDataSize || maxRows <= currentRows ){
      fileWriter.append( currentSpread );
      currentSpread = new Spread();
      currentDataSize = 0;
      currentRows = 0;
    }
  }

  public void close() throws IOException{
    if( currentSpread.size() != 0 ){
      fileWriter.append( currentSpread );
    }
    fileWriter.close();
  }

}
