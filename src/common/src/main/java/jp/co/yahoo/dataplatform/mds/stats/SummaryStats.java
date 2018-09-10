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
package jp.co.yahoo.dataplatform.mds.stats;

import java.util.Map;
import java.util.LinkedHashMap;

public class SummaryStats{

  private int statsCount;
  private long rowCount;
  private long rawDataSize;
  private long realDataSize;
  private long logicalDataSize;
  private long cardinality;

  public SummaryStats(){}

  public SummaryStats( final long rowCount , final long rawDataSize , final long realDataSize , final long logicalDataSize , final int cardinality ){
    this.rowCount = rowCount;
    this.rawDataSize = rawDataSize;
    this.realDataSize = realDataSize;
    this.logicalDataSize = logicalDataSize;
    this.cardinality = cardinality;
    statsCount = 1;
  }

  public void merge( final SummaryStats summaryStats ){
    this.rowCount += summaryStats.getRowCount();
    this.rawDataSize += summaryStats.getRawDataSize();
    this.realDataSize += summaryStats.getRealDataSize();
    this.logicalDataSize += summaryStats.getLogicalDataSize();
    this.cardinality += summaryStats.getCardinality();
    statsCount++;
  }

  public int getStatsCount(){
    return statsCount;
  }

  public long getRowCount(){
    return rowCount;
  }

  public long getRawDataSize(){
    return rawDataSize;
  }

  public long getRealDataSize(){
    return realDataSize;
  }

  public long getLogicalDataSize(){
    return logicalDataSize;
  }

  public long getCardinality(){
    return cardinality;
  }

  public double getAverageFieldSize(){
    return (double)rawDataSize / (double)rowCount;
  }

  public double getAverageFieldRealSize(){
    return (double)realDataSize / (double)rowCount;
  }

  public double getCompressLate(){
    return (double)realDataSize / (double)rawDataSize;
  }

  public double getAverageRowCount(){
    return (double)rowCount / (double)statsCount;
  }

  public double getAverageCardinality(){
    return (double)cardinality / (double)statsCount;
  }

  public Map<Object,Object> toJavaObject(){
    Map<Object,Object> result = new LinkedHashMap();
    result.put( "field_count" , rowCount );
    result.put( "raw_data_size" , rawDataSize );
    result.put( "real_data_size" , realDataSize );
    result.put( "logical_data_size" , logicalDataSize );
    result.put( "cardinality" , cardinality );
    result.put( "statsCount" , statsCount );

    return result;
  }

  public void clear(){
    rowCount = 0; 
    rawDataSize = 0;
    realDataSize = 0;
    logicalDataSize = 0;
    cardinality = 0;
  }

  @Override
  public String toString(){
    return String.format( "Field count=%d , Raw data size=%d , Real data size=%d , Logical data size=%d , cardinality=%d , stats report count=%d , Average field size=%f , Average field real size=%f , Compress late=%f , Average row count per stats report count=%f , Average cardinality=%f" , rowCount , rawDataSize , realDataSize , logicalDataSize , cardinality , statsCount , getAverageFieldSize() , getAverageFieldRealSize() , getCompressLate() , getAverageRowCount() , getAverageCardinality() );
  }

}
