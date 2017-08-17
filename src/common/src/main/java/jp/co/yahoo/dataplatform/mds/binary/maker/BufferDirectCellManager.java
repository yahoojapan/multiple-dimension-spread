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
package jp.co.yahoo.dataplatform.mds.binary.maker;

import java.io.IOException;
import java.io.UncheckedIOException;

import java.util.List;
import java.util.ArrayList;

import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.ICellManager;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveCell;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.IFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.index.DefaultCellIndex;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionIndex;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.column.index.ICellIndex;

public class BufferDirectCellManager implements ICellManager {

  private final ColumnType columnType;
  private final IDicManager dicManager;
  private final int indexSize;

  private ICellIndex index = new DefaultCellIndex();

  public BufferDirectCellManager( final ColumnType columnType , final IDicManager dicManager , final int indexSize ){
    this.columnType = columnType;
    this.dicManager = dicManager;
    this.indexSize = indexSize;
  }

  @Override
  public void add(final ICell cell , final int index ){
    throw new UnsupportedOperationException( "read only." );
  }

  @Override
  public ICell get( final int index , final ICell defaultCell ){
    if( indexSize <= index ){
      return defaultCell;
    }
    try{
      PrimitiveObject obj = dicManager.get( index );
      if( obj == null ){
        return defaultCell;
      }
      return new PrimitiveCell( columnType , dicManager.get( index ) );
    }catch( IOException e ){
      throw new RuntimeException( e );
    }
  }

  @Override
  public int getMaxIndex(){
    return indexSize - 1;
  }

  @Override
  public int size(){
    return indexSize;
  }

  @Override
  public void clear(){
  }

  @Override
  public void setIndex( final ICellIndex index ){
    this.index = index;
  }

  @Override
  public List<Integer> filter( final IFilter filter ) throws IOException{
    switch( filter.getFilterType() ){
      case NOT_NULL:
        return null;
      case NULL:
        return new ArrayList<Integer>( size() );
      default:
        return index.filter( filter );
    }
  }

  @Override
  public PrimitiveObject[] getPrimitiveObjectArray(final IExpressionIndex indexList , final int start , final int length ){
    PrimitiveObject[] result = new PrimitiveObject[length];
    int loopEnd = ( start + length );
    if( indexList.size() < loopEnd ){
      loopEnd = indexList.size();
    }
    for( int i = start , index = 0 ; i < loopEnd ; i++,index++ ){
      int targetIndex = indexList.get( i );
      if( indexSize <= targetIndex ){
        break;
      }
      try{
        result[index] = dicManager.get( targetIndex );
      }catch( IOException e ){
        throw new UncheckedIOException( e );
      }
    }
    return result;
  }

}
