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
import java.io.DataOutput;
import java.io.DataInput;

import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import org.apache.hadoop.io.Writable;

public class ColumnAndIndex implements Writable{

  public IColumn column;
  public int index;
  public int columnIndex;

  public ColumnAndIndex( final IColumn column , final int index , final int columnIndex ){
    this.column = column;
    this.index = index;
    this.columnIndex = columnIndex;
  }

  public ColumnAndIndex(){
  }

  public ICell getCurrent(){
    return column.get( index );
  }

  @Override
  public void write( final DataOutput dataOutput ) throws IOException {
    throw new UnsupportedOperationException("write unsupported");
  }

  @Override
  public void readFields( final DataInput dataInput ) throws IOException {
    throw new UnsupportedOperationException("readFields unsupported");
  }

}
