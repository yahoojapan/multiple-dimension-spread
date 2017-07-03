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
package jp.co.yahoo.dataplatform.mds.spread.expand;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.IntStream;

import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.schema.design.IField;
import jp.co.yahoo.dataplatform.schema.design.StructContainerField;
import jp.co.yahoo.dataplatform.schema.parser.IParser;

import jp.co.yahoo.dataplatform.mds.spread.column.NullColumn;

public class ExpandSpread extends Spread {

  private final Map<String,IColumn> expandColumn;
  private final List<IColumn> expandColumnList;
  private final Map<String,Integer> expandColumnIndexMap;
  private int[] spreadIndexArray;
  private Spread expandSpread;

  public ExpandSpread(){
    super();
    expandColumn = new LinkedHashMap<String,IColumn>();
    expandColumnList = new ArrayList<IColumn>();
    expandColumnIndexMap = new HashMap<String,Integer>();
  }

  public void setOriginalSpread( final Spread original , final int[] spreadIndexArray ){
    expandSpread = original;
    this.spreadIndexArray = spreadIndexArray;
  }

  public void addExpandLeafColumn( final String linkColumnName , final IColumn arrayColumn ){
    expandColumn.put( linkColumnName , arrayColumn );
    expandColumnList.add( arrayColumn );
    expandColumnIndexMap.put( linkColumnName , expandColumnIndexMap.size() );
  }

  public void addExpandColumn( final String linkColumnName , final IColumn arrayColumn , final int[] indexArray ){
    IColumn column = new ExpandColumn( arrayColumn , indexArray );
    expandColumn.put( linkColumnName , column );
    expandColumnList.add( column );
    expandColumnIndexMap.put( linkColumnName , expandColumnIndexMap.size() );
  }

  public Map<String,ICell> getLine(final Map<String,ICell> previous , final int index ){
    Map<String,ICell> result = expandSpread.getLine( previous , spreadIndexArray[index] );
    for( Map.Entry<String,IColumn> entry : expandColumn.entrySet() ){
      result.put( entry.getKey() , entry.getValue().get( index ) );
    }

    return result;
  }

  @Override
  public int addRow( final String key , final Object row ) throws IOException{
    throw new UnsupportedOperationException( "Expand spread is read only." );
  }

  @Override
  public int addRow( final Map<String,Object> row ){
    throw new UnsupportedOperationException( "Expand spread is read only." );
  }

  @Override
  public int addParserRow( final IParser parser )throws IOException{
    throw new UnsupportedOperationException( "Expand spread is read only." );
  }

  @Override
  public int addRows( final List<Map<String,Object>> rows ){
    throw new UnsupportedOperationException( "Expand spread is read only." );
  }

  public List<IColumn> getListColumn(){
    List<IColumn> result = new ArrayList<IColumn>();
    for( IColumn column : expandSpread.getListColumn() ){
      result.add( new ExpandColumn( column , spreadIndexArray ) );
    }
    result.addAll( expandColumnList );
    return result;
  }

  public Map<String,IColumn> getAllColumn(){
    Map<String,IColumn> result = new HashMap<String,IColumn>();
    for( Map.Entry<String,IColumn> entry : expandSpread.getAllColumn().entrySet() ){
      result.put( entry.getKey() , new ExpandColumn( entry.getValue() , spreadIndexArray ) );
    }
    result.putAll( expandColumn );
    return result;
  }

  public List<String> getColumnKeys(){
    List<String> result = expandSpread.getColumnKeys();
    for( Map.Entry<String,IColumn> entry : expandColumn.entrySet() ){
      result.add( entry.getKey() );
    }

    return result;
  }

  public IColumn getColumn( final int index ){
    int spreadSize = expandSpread.getColumnSize();
    if( spreadSize <= index ){
      return NullColumn.getInstance();
    }
    else if( index < spreadSize ){
      return new ExpandColumn( expandSpread.getColumn( index ) , spreadIndexArray );
    }
    else if( index < ( spreadSize + expandColumnList.size() ) ){
      return expandColumnList.get( index - expandSpread.getColumnSize() );
    }
    else{
      return NullColumn.getInstance();
    }
  }

  public IColumn getColumn( final String columnName ){
    if( expandColumn.containsKey( columnName ) ){
      return expandColumn.get( columnName );
    }
    return new ExpandColumn( expandSpread.getColumn( columnName ) , spreadIndexArray );
  }

  @Override
  public void addColumn( final IColumn column ){
    throw new UnsupportedOperationException( "Expand spread is read only." );
  }

  public boolean containsColumn( final String columnName ){
    return ( expandColumn.containsKey( columnName ) || expandSpread.containsColumn( columnName ) );
  }

  public int getColumnIndex( final String columnName ){
    if( expandColumn.containsKey( columnName ) ){
      return expandColumnIndexMap.get( columnName ) + expandSpread.getColumnSize(); 
    }
    return expandSpread.getColumnIndex( columnName );
  }

  public void setRowCount( final int rowCount ){
    throw new UnsupportedOperationException( "Expand spread is read only." );
  }

  public int getColumnSize(){
    return expandColumn.size() + expandSpread.getColumnSize();
  }

  public int size(){
    return spreadIndexArray.length;
  }

  public IField getSchema() throws IOException{
    return getSchema( "root" );
  }

  public IField getSchema( final String schemaName ) throws IOException{
    StructContainerField schema = (StructContainerField)expandSpread.getSchema();
    for( Map.Entry<String,IColumn> entry : expandColumn.entrySet() ){
      schema.set( entry.getValue().getSchema() );
    }
    return schema;
  }

  @Override
  public String toString(){
    StringBuffer result = new StringBuffer();
    Map<String,ICell> cache = new HashMap<String,ICell>();
    IntStream.range( 0 , size() )
      .forEach( i -> {
        Map<String,ICell> line = getLine( cache , i );
        result.append( "--------------------------\n" );
        result.append( String.format( "LINE-%d\n" , i ) );
        result.append( "--------------------------\n" );
        result.append( line.toString() );
        result.append( "\n" );
      } );

    return result.toString();
  }

}
