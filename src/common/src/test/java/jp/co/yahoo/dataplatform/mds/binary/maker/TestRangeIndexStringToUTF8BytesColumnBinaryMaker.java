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
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

import jp.co.yahoo.dataplatform.schema.objects.StringObj;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;

public class TestRangeIndexStringToUTF8BytesColumnBinaryMaker {

  private class TestStringMemoryAllocator implements IMemoryAllocator{

    public final List<String> list;

    public TestStringMemoryAllocator(){
      list = new ArrayList<String>();
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
      list.set( index , new String( value , start , length , "UTF-8" ) );
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
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "STRING" );
    column.add( ColumnType.STRING , new StringObj( "a" ) , 0 );
    column.add( ColumnType.STRING , new StringObj( "b" ) , 1 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new RangeIndexStringToUTF8BytesColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column , new MakerCache() );

    assertEquals( columnBinary.columnName , "STRING" );
    assertEquals( columnBinary.rowCount , 2 );
    assertEquals( columnBinary.columnType , ColumnType.STRING );

    IColumn decodeColumn = maker.toColumn( columnBinary , new DefaultPrimitiveObjectConnector() );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertEquals( "a" , ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getString() );
    assertEquals( "b" , ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getString() );

    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );
  }

  @Test
  public void T_loadInMemoryStorage_1() throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "STRING" );
    column.add( ColumnType.STRING , new StringObj( "a" ) , 0 );
    column.add( ColumnType.STRING , new StringObj( "b" ) , 1 );
    column.add( ColumnType.STRING , new StringObj( "c" ) , 5 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new RangeIndexStringToUTF8BytesColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column , new MakerCache() );

    assertEquals( columnBinary.columnName , "STRING" );
    assertEquals( columnBinary.rowCount , 3 );
    assertEquals( columnBinary.columnType , ColumnType.STRING );

    TestStringMemoryAllocator allocator = new TestStringMemoryAllocator();
    maker.loadInMemoryStorage( columnBinary , allocator );

    assertEquals( allocator.list.get(0) , "a" );
    assertEquals( allocator.list.get(1) , "b" );
    assertEquals( allocator.list.get(2) , null );
    assertEquals( allocator.list.get(3) , null );
    assertEquals( allocator.list.get(4) , null );
    assertEquals( allocator.list.get(5) , "c" );
  }

}
