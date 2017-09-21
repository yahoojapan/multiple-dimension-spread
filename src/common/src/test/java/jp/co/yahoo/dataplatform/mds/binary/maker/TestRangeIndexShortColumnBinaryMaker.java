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

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;

import jp.co.yahoo.dataplatform.schema.objects.ShortObj;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class TestRangeIndexShortColumnBinaryMaker {

  private class TestShortMemoryAllocator implements IMemoryAllocator{

    public final List<Short> list;

    public TestShortMemoryAllocator(){
      list = new ArrayList<Short>();
      for( int i = 0 ; i < 6 ; i++ ){
        list.add( null );
      }
    }

    @Override
    public void setNull( final int index ){
    }

    @Override
    public void setBoolean( final int index , final boolean value ) throws IOException{
    }

    @Override
    public void setByte( final int index , final byte value ) throws IOException{
    }

    @Override
    public void setShort( final int index , final short value ) throws IOException{
      list.set( index , value );
    }

    @Override
    public void setInteger( final int index , final int value ) throws IOException{
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
    IColumn column = new PrimitiveColumn( ColumnType.SHORT , "SHORT" );
    column.add( ColumnType.SHORT , new ShortObj( (short)1 ) , 0 );
    column.add( ColumnType.SHORT , new ShortObj( (short)2 ) , 1 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new RangeIndexShortColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column , new MakerCache() );

    assertEquals( columnBinary.columnName , "SHORT" );
    assertEquals( columnBinary.rowCount , 2 );
    assertEquals( columnBinary.columnType , ColumnType.SHORT );

    IColumn decodeColumn = maker.toColumn( columnBinary );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertEquals( (short)1 , ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getShort() );
    assertEquals( (short)2 , ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getShort() );

    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );
  }

  @Test
  public void T_loadInMemoryStorage_1() throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.SHORT , "SHORT" );
    column.add( ColumnType.SHORT , new ShortObj( (short)1 ) , 0 );
    column.add( ColumnType.SHORT , new ShortObj( (short)2 ) , 1 );
    column.add( ColumnType.SHORT , new ShortObj( (short)5 ) , 5 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new RangeIndexShortColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column , new MakerCache() );

    assertEquals( columnBinary.columnName , "SHORT" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.SHORT );

    TestShortMemoryAllocator allocator = new TestShortMemoryAllocator();
    maker.loadInMemoryStorage( columnBinary , allocator );
    assertEquals( (short)1 , allocator.list.get(0).shortValue() );
    assertEquals( (short)2 , allocator.list.get(1).shortValue() );
    assertEquals( null , allocator.list.get(2) );
    assertEquals( null , allocator.list.get(3) );
    assertEquals( null , allocator.list.get(4) );
    assertEquals( (short)5 , allocator.list.get(5).shortValue() );
  }

}

