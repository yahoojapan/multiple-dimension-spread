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

import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

import jp.co.yahoo.dataplatform.schema.objects.IntegerObj;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

public class TestRangeIndexIntegerColumnBinaryMaker {

  private class TestIntegerMemoryAllocator implements IMemoryAllocator{

    public final List<Integer> list;

    public TestIntegerMemoryAllocator(){
      list = new ArrayList<Integer>();
      for( int i = 0 ; i < 6 ; i++ ){
        list.add( null );
      }
    }

    @Override
    public void setNull( final int index ) throws IOException{
    }

    @Override
    public void setBoolean( final int index , final boolean value ) throws IOException{
    }

    @Override
    public void setByte( final int index , final byte value ) throws IOException{
    }

    @Override
    public void setShort( final int index , final short value ) throws IOException{
    }

    @Override
    public void setInteger( final int index , final int value ) throws IOException{
      list.set( index , value );
    }

    @Override
    public void setLong( final int index , final long value ) throws IOException{
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
    IColumn column = new PrimitiveColumn( ColumnType.INTEGER , "INTEGER" );
    column.add( ColumnType.INTEGER , new IntegerObj( 1 ) , 0 );
    column.add( ColumnType.INTEGER , new IntegerObj( 2 ) , 1 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new RangeIndexIntegerColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column , new MakerCache() );

    assertEquals( columnBinary.columnName , "INTEGER" );
    assertEquals( columnBinary.rowCount , 2 );
    assertEquals( columnBinary.columnType , ColumnType.INTEGER );

    IColumn decodeColumn = maker.toColumn( columnBinary , new DefaultPrimitiveObjectConnector() );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertEquals( 1 , ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getInt() );
    assertEquals( 2 , ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getInt() );

    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );
  }

  @Test
  public void T_loadInMemoryStorage_1() throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.INTEGER , "INTEGER" );
    column.add( ColumnType.INTEGER , new IntegerObj( 1 ) , 0 );
    column.add( ColumnType.INTEGER , new IntegerObj( 2 ) , 1 );
    column.add( ColumnType.INTEGER , new IntegerObj( 5 ) , 5 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new RangeIndexIntegerColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column , new MakerCache() );

    assertEquals( columnBinary.columnName , "INTEGER" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.INTEGER );

    TestIntegerMemoryAllocator allocator = new TestIntegerMemoryAllocator();
    maker.loadInMemoryStorage( columnBinary , allocator );
    assertEquals( 1 , allocator.list.get(0).intValue() );
    assertEquals( 2 , allocator.list.get(1).intValue() );
    assertEquals( null , allocator.list.get(2) );
    assertEquals( null , allocator.list.get(3) );
    assertEquals( null , allocator.list.get(4) );
    assertEquals( 5 , allocator.list.get(5).intValue() );
  }

}
