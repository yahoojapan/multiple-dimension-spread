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

import java.io.IOException;

import java.util.Set;
import java.util.HashSet;

import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveCell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

public class DoubleColumnAnalizer implements IColumnAnalizer{

  private final IColumn column;

  public DoubleColumnAnalizer( final IColumn column ){
    this.column = column;
  }

  public IColumnAnalizeResult analize() throws IOException{
    boolean maybeSorted = true;
    Double currentSortCheckValue = Double.MIN_VALUE;
    int nullCount = 0;
    int rowCount = 0;

    Set<Double> dicSet = new HashSet<Double>();

    Double min = Double.MAX_VALUE;
    Double max = Double.MIN_VALUE;
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      if( cell.getType() == ColumnType.NULL ){
        nullCount++;
        continue;
      }
      Double target = Double.valueOf( ( (PrimitiveCell) cell).getRow().getDouble() );
      if( maybeSorted && 0 <= currentSortCheckValue.compareTo( target ) ){
        currentSortCheckValue = target;
      }
      else{
        maybeSorted = false;
      }

      rowCount++;
      if( ! dicSet.contains( target ) ){
        dicSet.add( target );
        if( 0 < min.compareTo( target ) ){
          min = Double.valueOf( target );
        }
        if( max.compareTo( target ) < 0 ){
          max = Double.valueOf( target );
        }
      }
    }

    int uniqCount = dicSet.size();

    return new DoubleColumnAnalizeResult( column.getColumnName() , column.size() , maybeSorted , nullCount , rowCount , uniqCount , min , max );
  }

}
