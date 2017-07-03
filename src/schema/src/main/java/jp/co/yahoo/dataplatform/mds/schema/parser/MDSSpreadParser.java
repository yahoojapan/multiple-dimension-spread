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

import java.util.Map;
import java.util.HashMap;

import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;
import jp.co.yahoo.dataplatform.schema.parser.IParser;

public class MDSSpreadParser implements ISettableIndexParser {

  private final IColumn column;
  private final Map<String,ISettableIndexParser> cache;

  private int currentIndex;

  public MDSSpreadParser( final IColumn column ){
    this.column = column;
    cache = new HashMap<String,ISettableIndexParser>();
  }

  @Override
  public void setIndex( final int index ){
    currentIndex = index;
  }

  @Override
  public PrimitiveObject get( final String key ) throws IOException{
    return PrimitiveConverter.convert( column.getColumn( key ).get( currentIndex ) );
  }

  @Override
  public PrimitiveObject get( final int index ) throws IOException{
    return null;
  }

  @Override
  public IParser getParser( final String key ) throws IOException{
    ISettableIndexParser parser = cache.get( key );
    if( parser == null ){
      parser = MDSParserFactory.get( column.getColumn( key ) , currentIndex );
      cache.put( key , parser );
    }
    parser.setIndex( currentIndex );
    
    return parser;
  }

  @Override
  public IParser getParser( final int index ) throws IOException{
    return MDSNullParser.getInstance();
  }

  @Override
  public String[] getAllKey() throws IOException{
    return column.getColumnKeys().toArray( new String[ column.getColumnSize() ] );
  }

  @Override
  public boolean containsKey( final String key ) throws IOException{
    return get( key ) != null;
  }

  @Override
  public int size() throws IOException{
    return column.getColumnSize();
  }

  @Override
  public boolean isArray() throws IOException{
    return false;
  }

  @Override
  public boolean isMap() throws IOException{
    return true;
  }

  @Override
  public boolean isStruct() throws IOException{
    return false;
  }

  @Override
  public boolean hasParser( final int index ) throws IOException{
    return false;
  }

  @Override
  public boolean hasParser( final String key ) throws IOException{
    return MDSParserFactory.hasParser( column.getColumn( key ) , currentIndex );
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
