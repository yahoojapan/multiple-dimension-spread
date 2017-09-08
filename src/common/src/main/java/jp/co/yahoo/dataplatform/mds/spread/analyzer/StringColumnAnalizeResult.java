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
package jp.co.yahoo.dataplatform.mds.spread.analyzer;

import jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

public class StringColumnAnalizeResult implements IColumnAnalizeResult{

  private final String columnName;
  private final int columnSize;
  private final boolean sortFlag;
  private final int nullCount;
  private final int rowCount;
  private final int uniqCount;

  int totalLogicalDataSize;
  int totalUtf8ByteSize;
  int uniqLogicalDataSize;
  int uniqUtf8ByteSize;

  int minCharLength;
  int maxCharLength;
  int minUtfBytes;
  int maxUtfBytes;

  private final String min;
  private final String max;

  public StringColumnAnalizeResult( final String columnName , final int columnSize , final boolean sortFlag , final int nullCount , final int rowCount , final int uniqCount , final int totalLogicalDataSize , final int totalUtf8ByteSize , final int uniqLogicalDataSize , final int uniqUtf8ByteSize , final int minCharLength , final int maxCharLength , final int minUtfBytes , final int maxUtfBytes , final String min , final String max ){
    this.columnName = columnName;
    this.columnSize = columnSize;
    this.sortFlag = sortFlag;
    this.nullCount = nullCount;
    this.rowCount = rowCount;
    this.uniqCount = uniqCount;
    this.totalLogicalDataSize = totalLogicalDataSize;
    this.totalUtf8ByteSize = totalUtf8ByteSize;
    this.uniqLogicalDataSize = uniqLogicalDataSize;
    this.uniqUtf8ByteSize = uniqUtf8ByteSize;
    this.minCharLength = minCharLength;
    this.maxCharLength = maxCharLength;
    this.minUtfBytes = minUtfBytes;
    this.maxUtfBytes = maxUtfBytes;
    this.min = min;
    this.max = max;
  }

  @Override
  public String getColumnName(){
    return columnName;
  }

  @Override
  public ColumnType getColumnType(){
    return ColumnType.STRING;
  }

  @Override
  public int getColumnSize(){
    return columnSize;
  }

  @Override
  public boolean maybeSorted(){
    return sortFlag;
  }

  @Override
  public int getNullCount(){
    return nullCount;
  }

  @Override
  public int getRowCount(){
    return rowCount;
  }

  @Override
  public int getUniqCount(){
    return uniqCount;
  }

  @Override
  public int getLogicalDataSize(){
    return totalLogicalDataSize;
  }

  public int getTotalUtf8ByteSize(){
    return totalUtf8ByteSize;
  }

  public int getUniqLogicalDataSize(){
    return uniqLogicalDataSize;
  }

  public int getUniqUtf8ByteSize(){
    return uniqUtf8ByteSize;
  }

  public int getMinCharLength(){
    return minCharLength;
  }

  public int getMaxCharLength(){
    return maxCharLength;
  }

  public int getMinUtf8Bytes(){
    return minUtfBytes;
  }

  public int getMaxUtf8Bytes(){
    return maxUtfBytes;
  }

  public String getMin(){
    return min;
  }

  public String getMax(){
    return max;
  }

}
