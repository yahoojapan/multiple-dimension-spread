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
package jp.co.yahoo.dataplatform.mds.spread;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.DataProvider;

import jp.co.yahoo.dataplatform.schema.objects.*;

import jp.co.yahoo.dataplatform.mds.spread.column.NullColumn;

public class TestSpread {

    @DataProvider(name = "D_T_addRowNotException")
    public Object[][] D_T_addRowNotException() {
        return new Object[][] {
                { new BooleanObj(false) },
                { new ByteObj((byte) 0) },
                { new BytesObj(new byte[0]) },
                { new DoubleObj((double) 0) },
                { new FloatObj((float) 0) },
                { new IntegerObj(0) },
                { new LongObj((long) 0) },
                { new ShortObj((short) 0) },
                { new StringObj("") },
                { new HashMap<String, Object>() },
                { new ArrayList<Object>() },
        };
    }

    @DataProvider(name = "D_T_addRowException")
    public Object[][] D_T_addRowException() {
        return new Object[][] {
                { "aaa" },
                { 0 },
        };
    }

    @Test
    public void T_constructTest_1() {
        Spread spread = new Spread();
    }

    @Test
    public void T_constructTest_2() {
        Spread spread = new Spread(NullColumn.getInstance());
    }

    @Test(expectedExceptions = { java.io.IOException.class }, dataProvider = "D_T_addRowException")
    public void T_addRowException(final Object target) throws IOException {
        Spread spread = new Spread();
        spread.addRow("col1", target);
    }

    @Test
    public void T_addRows() throws IOException {
        ArrayList<Map<String,Object>> rows = new ArrayList<>();
        Map<String, Object> dataContainer0 = new HashMap<String, Object>();
        Map<String, Object> dataContainer1 = new HashMap<String, Object>();
        Map<String, Object> dataContainer2 = new HashMap<String, Object>();
        dataContainer0.put("strColumn", new StringObj("row0"));
        dataContainer1.put("strColumn", new StringObj("row1"));
        dataContainer2.put("strColumn", new StringObj("row2"));
        dataContainer0.put("intColumn", new IntegerObj(0));
        dataContainer1.put("intColumn", new IntegerObj(1));
        dataContainer2.put("intColumn", new IntegerObj(2));
        rows.add(dataContainer0);
        rows.add(dataContainer1);
        rows.add(dataContainer2);
        Spread spread = new Spread();
        spread.addRows(rows);
        assertEquals(spread.getColumnSize(), 2);
        Assert.assertEquals(spread.getColumn("strColumn").get(0).toString(), "(STRING)row0");
        Assert.assertEquals(spread.getColumn("strColumn").get(1).toString(), "(STRING)row1");
        Assert.assertEquals(spread.getColumn("strColumn").get(2).toString(), "(STRING)row2");
        Assert.assertEquals(spread.getColumn("intColumn").get(0).toString(), "(INTEGER)0");
        Assert.assertEquals(spread.getColumn("intColumn").get(1).toString(), "(INTEGER)1");
        Assert.assertEquals(spread.getColumn("intColumn").get(2).toString(), "(INTEGER)2");
    }

    @Test(dataProvider = "D_T_addRowNotException")
    public void T_getAllColumnNotException(final Object target) throws IOException {
        Spread spread = new Spread();
        spread.addRow("col1", target);
        spread.getAllColumn();
    }

    @Test
    public void T_getAllColumn() throws IOException {
        Map<String, Object> dataContainer = new HashMap<String, Object>();
        dataContainer.put("boolean", new BooleanObj(false));
        dataContainer.put("byte",    new ByteObj((byte) 0));
        dataContainer.put("bytes",   new BytesObj(new byte[0]));
        dataContainer.put("double",  new DoubleObj((double) 0));
        dataContainer.put("float",   new FloatObj((float) 0));
        dataContainer.put("integer", new IntegerObj(5));
        dataContainer.put("long",    new LongObj((long) 0));
        dataContainer.put("short",   new ShortObj((short) 0));
        dataContainer.put("string",  new StringObj("val0"));
        Spread spread = new Spread();
        spread.addRow(dataContainer);
        assertEquals(spread.getAllColumn().size(), 9);
        Assert.assertEquals(spread.getAllColumn().get("string").get(0).toString(), "(STRING)val0");
        assertEquals(spread.getAllColumn().containsKey("double"), true);
        assertEquals(spread.getAllColumn().containsKey("hogehoge"), false);
    }

    @Test(dataProvider = "D_T_addRowNotException")
    public void T_getColumnNotException(final Object target) throws IOException {
        Spread spread = new Spread();
        spread.addRow("col1", target);
        spread.getColumn("col1");
    }

    @Test
    public void T_getColumn() throws IOException {
        Map<String, Object> dataContainer = new HashMap<String, Object>();
        dataContainer.put("string",  new StringObj("val0"));
        Spread spread = new Spread();
        spread.addRow(dataContainer);
        Assert.assertEquals(spread.getColumn("string").get(0).toString(), "(STRING)val0");
        Assert.assertEquals(spread.getColumn(0).get(0).toString(), "(STRING)val0");
        assertEquals(spread.getColumn(1),NullColumn.getInstance());
    }

    @Test
    public void T_toString() throws IOException {
        Map<String, Object> dataContainer = new HashMap<String, Object>();
        dataContainer.put("stringColumn",  new StringObj("val0"));
        dataContainer.put("intColumn",  new IntegerObj(123));
        Spread spread = new Spread();
        spread.addRow(dataContainer);
        assertEquals(spread.toString(),
                  "--------------------------\n"
                + "LINE-0\n"
                + "--------------------------\n"
                + "{stringColumn=(STRING)val0, intColumn=(INTEGER)123}\n"
                );
    }

/*

  public Map<String,ICell> getLine( final Map<String,ICell> previous , final int index ){
    Map<String,ICell> result = previous;
    if( result == null ){
      result = new HashMap<String,ICell>( columnList.size() );
    }
    else{
      result.clear();
    }

    for( IColumn column : columnList ){
      result.put( column.getColumnName() , column.get( index ) );
    }

    return result;
  }

  private void registerRow( final String columnName , final Object row ) throws IOException{
    ColumnType type = ColumnTypeFactory.get( row );
    switch( type ){
      case EMPTY_SPREAD:
      case EMPTY_ARRAY:
      case NULL:
        return;
      default:
    }

    int index = getColumnIndex( columnName );

    if( index == -1 ){
      IColumn column = ColumnFactory.get( type , columnName );
      column.setParentsColumn( parentColumn );
      columnIndexMapping.put( columnName , Integer.valueOf( columnList.size() ) );
      index = columnList.size();
      columnList.add( column );
    }
    IColumn column = columnList.get( index );
    if( column.getColumnType() != ColumnType.UNION && column.getColumnType() != type ){
      UnionColumn unionColumn = new UnionColumn( column );
      unionColumn.setParentsColumn( parentColumn );
      columnList.set( index , unionColumn );
      column = unionColumn;
    }
    column.add( type , row , rowCount );
  }

  public void addRow( final String key , final Object row ) throws IOException{
    registerRow( key , row );
    rowCount++;
  }

  public void addRow( final Map<String,Object> row ){
    row
      .forEach( ( k , v ) -> { 
        try{ 
          registerRow( k , v ); 
        }catch( IOException e ){
          throw new UncheckedIOException( "IOException addRow in lambda." , e );
        }
      } );
    rowCount++;
  }

  public void addParserRow( final IParser parser )throws IOException{
    String[] keys = parser.getAllKey();
    for( String key : keys ){
      if( parser.hasParser( key ) ){
        registerRow( key , parser.getParser( key ) );
      }
      else{
        registerRow( key , parser.get(key) );
      }
    }
    rowCount++;
  }

  public void addRows( final List<Map<String,Object>> rows ){
    rows.stream().forEach( row -> addRow( row ) );
  }

  public List<IColumn> getListColumn(){
    return columnList;
  }

  public Map<String,IColumn> getAllColumn(){
    Map<String,IColumn> result = new HashMap<String,IColumn>();
    columnList.stream().forEach( column -> result.put( column.getColumnName() , column ) );
    return result;
  }

  public List<String> getColumnKeys(){
    return new ArrayList<String>( columnIndexMapping.keySet() );
  }

  public IColumn getColumn( final int index ){
    if( columnList.size() <= index ){
      return NullColumn.getInstance();
    }
    return columnList.get( index );
  }

  public IColumn getColumn( final String columnName ){
    if( ! containsColumn( columnName ) ){
      return NullColumn.getInstance();
    }

    return getColumn( columnIndexMapping.get( columnName ).intValue() );
  }

  public void addColumn( final IColumn column , final int columnCount ){
    columnIndexMapping.put( column.getColumnName() , Integer.valueOf( columnList.size() ) );
    columnList.add( column );
    if( rowCount < columnCount ){
      rowCount = columnCount;
    }
  }

  public boolean containsColumn( final String columnName ){
    return columnIndexMapping.containsKey( columnName );
  }

  public int getColumnIndex( final String columnName ){
    if( ! containsColumn( columnName ) ){
      return -1;
    }

    return columnIndexMapping.get( columnName ).intValue();
  }

  public void setRowCount( final int rowCount ){
    this.rowCount = rowCount;
  }

  public int getColumnSize(){
    return columnList.size();
  }

  public int size(){
    return rowCount;
  }

  public IField getSchema() throws IOException{
    return getSchema( "root" );
  }

  public IField getSchema( final String schemaName ) throws IOException{
    StructContainerField schema = new StructContainerField( schemaName );
    columnList.stream()
      .forEach( column -> {
        try{
          schema.set( column.getSchema() ); 
        }catch( IOException e ){
          throw new UncheckedIOException( "IOException addRow in lambda." , e );
        }
      } );
    return schema;
  }

  @Override
  public String toString(){
    StringBuffer result = new StringBuffer();
    Map<String,ICell> cache = new HashMap<String,ICell>();
    IntStream.range( 0 , rowCount )
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
*/    

}
