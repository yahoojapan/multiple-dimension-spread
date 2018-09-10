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
package jp.co.yahoo.dataplatform.mds.tools;

import jp.co.yahoo.dataplatform.mds.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;
import jp.co.yahoo.dataplatform.mds.stats.SummaryStats;

public class ColumnMakerPerformanceResult{

  private final String columnPath;
  private final IColumnBinaryMaker maker;
  private final ICompressor compressor;
  private final SummaryStats stats;
  private final long toBinaryCpuTimeNano;
  private final long toColumnCpuTimeNano;

  public ColumnMakerPerformanceResult( final String columnPath , final IColumnBinaryMaker maker , final ICompressor compressor , final SummaryStats stats , final long toBinaryCpuTimeNano , final long toColumnCpuTimeNano ){
    this.columnPath = columnPath;
    this.maker = maker;
    this.compressor = compressor;
    this.stats = stats;
    this.toBinaryCpuTimeNano = toBinaryCpuTimeNano;
    this.toColumnCpuTimeNano = toColumnCpuTimeNano;
  }

  @Override
  public String toString(){
    return String.format( "%s,%s,%s,%d,%d,%d,%d,%.3f,%.3f" , 
      columnPath , 
      maker.getClass().getName() , 
      compressor.getClass().getName() , 
      stats.getRowCount() , 
      stats.getRawDataSize() , 
      stats.getRealDataSize() , 
      stats.getLogicalDataSize(),
      CpuTimeUtil.nanoToMill( toBinaryCpuTimeNano ),
      CpuTimeUtil.nanoToMill( toColumnCpuTimeNano )
    );
  }

}
