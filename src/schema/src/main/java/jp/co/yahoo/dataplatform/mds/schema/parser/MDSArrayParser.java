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
package jp.co.yahoo.dataplatform.mds.schema.parser;

import java.io.IOException;

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;
import jp.co.yahoo.dataplatform.schema.parser.IParser;

import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.ArrayCell;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

public class MDSArrayParser implements ISettableIndexParser {

  private final IColumn column;
  private final IColumn arrayColumn;

  private int start;
  private int length;

  public MDSArrayParser( final IColumn column ){
    this.column = column;
    arrayColumn = column.getColumn(0);
  }

  @Override
  public void setIndex( final int index ){
    ICell cell = column.get( index );
    if( cell.getType() == ColumnType.ARRAY ){
      ArrayCell arrayCell = (ArrayCell)cell;
      start = arrayCell.getStart();
      length = arrayCell.getEnd() - start;
    }
    else{
      start = 0;
      length = 0;
    }
  }

  @Override
  public PrimitiveObject get( final String key ) throws IOException{
    return null;
  }

  @Override
  public PrimitiveObject get( final int index ) throws IOException{
    if( length <= index ){
      return null;
    }
    return PrimitiveConverter.convert( arrayColumn.get( start + index ) );
  }

  @Override
  public IParser getParser( final String key ) throws IOException{
    return MDSNullParser.getInstance();
  }

  @Override
  public IParser getParser( final int index ) throws IOException{
    if( length <= index ){
      return MDSNullParser.getInstance();
    }
    int target = start + index;
    ISettableIndexParser parser = MDSParserFactory.get( arrayColumn , target );
    parser.setIndex( target );
    
    return parser;
  }

  @Override
  public String[] getAllKey() throws IOException{
    return new String[0];
  }

  @Override
  public boolean containsKey( final String key ) throws IOException{
    return false;
  }

  @Override
  public int size() throws IOException{
    return length;
  }

  @Override
  public boolean isArray() throws IOException{
    return true;
  }

  @Override
  public boolean isMap() throws IOException{
    return false;
  }

  @Override
  public boolean isStruct() throws IOException{
    return false;
  }

  @Override
  public boolean hasParser( final int index ) throws IOException{
    if( length <= index ){
      return false;
    }
    int target = start + index;
    return MDSParserFactory.hasParser( arrayColumn , target );
  }

  @Override
  public boolean hasParser( final String key ) throws IOException{
    return false;
  }

  @Override
  public Object toJavaObject() throws IOException{
    return null;
/*
    Map<String,Object> result = new HashMap<String,Object>();
    for( String key : getAllKey() ){
      if( hasParser(key) ){
        result.put( key , getParser(key).toJavaObject() );
      }
      else{
        result.put( key , get(key) );
      }
    }

    return result;
*/
  }

}
