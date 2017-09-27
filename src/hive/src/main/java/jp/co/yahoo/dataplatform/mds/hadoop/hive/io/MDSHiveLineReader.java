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
import java.io.InputStream;

import java.util.List;

import jp.co.yahoo.dataplatform.mds.MDSReader;
import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.spread.column.SpreadColumn;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionIndex;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.IndexFactory;
import jp.co.yahoo.dataplatform.mds.stats.SummaryStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;

public class MDSHiveLineReader implements RecordReader<NullWritable, ColumnAndIndex> {

  private static final Logger LOG = LoggerFactory.getLogger(MDSHiveLineReader.class);

  private final MDSReader reader;
  private final SpreadColumn spreadColumn = new SpreadColumn( "root" );
  private final IExpressionNode node;
  private final IJobReporter reporter;
  private final SpreadCounter spreadCounter;

  private Spread currentSpread;
  private int currentIndex;
  private IExpressionIndex currentIndexList;
  private boolean isEnd;
  private int readSpreadCount;

  public MDSHiveLineReader( final InputStream in , final long dataLength , final long start , final long length , final IReaderSetting setting , final IJobReporter reporter , final SpreadCounter spreadCounter ) throws IOException{
    this.reporter = reporter;
    this.spreadCounter = spreadCounter;
    reader = new MDSReader();
    node = setting.getExpressionNode();
    reader.setBlockSkipIndex( node );
    reader.setNewStream( in , dataLength , setting.getReaderConfig() , start , length );
    nextReader();
  }

  @Override
  public void close() throws IOException{
    reader.close();
  }

  @Override
  public NullWritable createKey(){
    return NullWritable.get();
  }

  @Override
  public ColumnAndIndex createValue() {
    return new ColumnAndIndex();
  }

  @Override
  public long getPos() throws IOException {
    return reader.getReadPos();
  }

  @Override
  public float getProgress() throws IOException {
    return (float)reader.getBlockReadCount() / (float)reader.getBlockCount();
  }

  private void updateCounter( final SummaryStats stats ){
    if( ! isEnd ){
      reporter.incrCounter( "MDS_STATS" , "ROWS" , stats.getRowCount() );
      reporter.incrCounter( "MDS_STATS" , "RAW_DATA_SIZE" , stats.getRawDataSize() );
      reporter.incrCounter( "MDS_STATS" , "REAL_DATA_SIZE" , stats.getRealDataSize() );
      reporter.incrCounter( "MDS_STATS" , "LOGICAL_DATA_SIZE" , stats.getLogicalDataSize() );
      reporter.incrCounter( "MDS_STATS" , "LOGICAL_TOTAL_CARDINALITY" , stats.getCardinality() );
      reporter.incrCounter( "MDS_STATS" , "SPREAD" , readSpreadCount );
    }
  }

  private boolean nextReader() throws IOException{
    if( ! reader.hasNext() ){
      currentSpread = null;
      currentIndex = 0;
      return false;
    }
    currentSpread = reader.next();
    readSpreadCount++;
    if( currentSpread.size() == 0 ){
      return nextReader();
    }
    spreadCounter.increment();
    currentIndexList = IndexFactory.toExpressionIndex( currentSpread , node.exec( currentSpread ) );
    currentIndex = 0;
    if( currentIndexList.size() == 0 ){
      return nextReader();
    }
    return true;
  }

  @Override
  public boolean next( final NullWritable key, final ColumnAndIndex value ) throws IOException {
    if( currentSpread == null || currentIndex == currentIndexList.size() ){
      if( ! nextReader() ){
        updateCounter( reader.getReadStats() );
        isEnd = true;
        return false;
      }
    }

    spreadColumn.setSpread( currentSpread );
    value.column = spreadColumn;
    value.index =  currentIndexList.get( currentIndex );
    value.columnIndex = spreadCounter.get();
    currentIndex++;
    return true;
  }

}
