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

import java.util.List;
import java.util.ArrayList;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;

import jp.co.yahoo.dataplatform.schema.objects.*;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class TestOptimizeDumpLongColumnBinaryMaker {

  private class TestIntegerMemoryAllocator implements IMemoryAllocator{

    public final long[] longArray;
    public final boolean[] nullArray;

    public TestIntegerMemoryAllocator(){
      longArray = new long[6];
      nullArray = new boolean[6];
    }

    @Override
    public void setNull( final int index ){
      nullArray[index] = true;
    }

    @Override
    public void setBoolean( final int index , final boolean value ) throws IOException{
    }

    @Override
    public void setByte( final int index , final byte value ) throws IOException{
      longArray[index] = value;
    }

    @Override
    public void setShort( final int index , final short value ) throws IOException{
      longArray[index] = value;
    }

    @Override
    public void setInteger( final int index , final int value ) throws IOException{
      longArray[index] = value;
    }

    @Override
    public void setLong( final int index , final long value ) throws IOException{
      longArray[index] = value;
    }

    @Override
    public void setFloat( final int index , final float value ) throws IOException{
    }

    @Override
    public void setDouble( final int index , final double value ) throws IOException{
    }

    @Override
    public void setBytes( final int index , final byte[] value ) throws IOException{
    }

    @Override
    public void setBytes( final int index , final byte[] value , final int start , final int length ) throws IOException{
    }

    @Override
    public void setString( final int index , final String value ) throws IOException{
    }

    @Override
    public void setString( final int index , final char[] value ) throws IOException{
    }

    @Override
    public void setString( final int index , final char[] value , final int start , final int length ) throws IOException{
    }

    @Override
    public void setValueCount( final int index ) throws IOException{

    }

    @Override
    public int getValueCount() throws IOException{
      return 0;
    }

    @Override
    public void setArrayIndex( final int index , final int start , final int end ) throws IOException{
    }

    @Override
    public IMemoryAllocator getChild( final String columnName , final ColumnType type ) throws IOException{
      return null;
    }
  }

  @Test
  public void T_toBinary_1() throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.SHORT , "SHORT" );
    column.add( ColumnType.SHORT , new ShortObj( (short)1 ) , 0 );
    column.add( ColumnType.SHORT , new ShortObj( (short)2 ) , 1 );
    column.add( ColumnType.SHORT , new ShortObj( (short)6 ) , 5 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new OptimizeDumpLongColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );

    assertEquals( columnBinary.columnName , "SHORT" );
    assertEquals( columnBinary.rowCount , 6 );
    assertEquals( columnBinary.columnType , ColumnType.SHORT );

    IColumn decodeColumn = maker.toColumn( columnBinary );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertEquals( (short)1 , ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getShort() );
    assertEquals( (short)2 , ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getShort() );
    assertEquals( (short)6 , ( (PrimitiveObject)( decodeColumn.get(5).getRow() ) ).getShort() );

    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );
  }

  @Test
  public void T_toBinary_2() throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.INTEGER , "C" );
    column.add( ColumnType.INTEGER , new IntegerObj( 1488553199 ) , 0 );
    column.add( ColumnType.INTEGER , new IntegerObj( 1488553299 ) , 1 );
    column.add( ColumnType.INTEGER , new IntegerObj( 1488653299 ) , 2 );
    column.add( ColumnType.INTEGER , new IntegerObj( 1488653299 ) , 5 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new OptimizeDumpLongColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );

    assertEquals( columnBinary.columnName , "C" );
    assertEquals( columnBinary.rowCount , 6 );
    assertEquals( columnBinary.columnType , ColumnType.INTEGER );

    IColumn decodeColumn = maker.toColumn( columnBinary );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertEquals( 1488553199 , ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getInt() );
    assertEquals( 1488553299 , ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getInt() );
    assertEquals( 1488653299 , ( (PrimitiveObject)( decodeColumn.get(2).getRow() ) ).getInt() );
    assertEquals( 1488653299 , ( (PrimitiveObject)( decodeColumn.get(5).getRow() ) ).getInt() );

    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );
  }

  @Test
  public void T_loadInMemoryStorage_1() throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.INTEGER , "INTEGER" );
    column.add( ColumnType.INTEGER , new IntegerObj( 10 ) , 0 );
    column.add( ColumnType.INTEGER , new IntegerObj( 2 ) , 1 );
    column.add( ColumnType.INTEGER , new IntegerObj( 6 ) , 5 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new OptimizeDumpLongColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );

    assertEquals( columnBinary.columnName , "INTEGER" );
    assertEquals( columnBinary.rowCount , 6 );
    assertEquals( columnBinary.columnType , ColumnType.INTEGER );

    TestIntegerMemoryAllocator allocator = new TestIntegerMemoryAllocator();
    maker.loadInMemoryStorage( columnBinary , allocator );
    assertEquals( 10 , allocator.longArray[0] );
    assertEquals( 2 , allocator.longArray[1] );
    assertTrue( allocator.nullArray[2] );
    assertTrue( allocator.nullArray[3] );
    assertTrue( allocator.nullArray[4] );
    assertEquals( 6 , allocator.longArray[5] );
  }

}
