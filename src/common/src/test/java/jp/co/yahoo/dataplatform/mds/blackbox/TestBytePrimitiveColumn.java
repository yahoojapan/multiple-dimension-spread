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
package jp.co.yahoo.dataplatform.mds.blackbox;

import java.io.IOException;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

import jp.co.yahoo.dataplatform.config.Configuration;

import jp.co.yahoo.dataplatform.schema.objects.*;

import jp.co.yahoo.dataplatform.mds.spread.expression.*;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.*;
import jp.co.yahoo.dataplatform.mds.spread.column.*;
import jp.co.yahoo.dataplatform.mds.binary.*;
import jp.co.yahoo.dataplatform.mds.binary.maker.*;

public class TestBytePrimitiveColumn {

  @DataProvider(name = "target_class")
  public Object[][] data1() throws IOException{
    return new Object[][] {
      { "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeDumpLongColumnBinaryMaker" },
      { "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeLongColumnBinaryMaker" },
      { "jp.co.yahoo.dataplatform.mds.binary.maker.UnsafeOptimizeDumpLongColumnBinaryMaker" },
      { "jp.co.yahoo.dataplatform.mds.binary.maker.UnsafeOptimizeLongColumnBinaryMaker" },
    };
  }

  public IColumn createNotNullColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BYTE , "column" );
    column.add( ColumnType.BYTE , new ByteObj( Byte.MAX_VALUE ) , 0 );
    column.add( ColumnType.BYTE , new ByteObj( Byte.MIN_VALUE ) , 1 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-2 ) , 2 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-3 ) , 3 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-4 ) , 4 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-5 ) , 5 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-6 ) , 6 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)7 ) , 7 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)8 ) , 8 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)9 ) , 9 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)0 ) , 10 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );
    return FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  public IColumn createNullColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BYTE , "column" );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );
    return  FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  public IColumn createHasNullColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BYTE , "column" );
    column.add( ColumnType.BYTE , new ByteObj( (byte)0 ) , 0 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)4 ) , 4 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)8 ) , 8 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );
    return FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  public IColumn createLastCellColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BYTE , "column" );
    column.add( ColumnType.BYTE , new ByteObj( Byte.MAX_VALUE ) , 10000 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );
    return FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  @Test( dataProvider = "target_class" )
  public void T_notNull_1( final String targetClassName ) throws IOException{
    IColumn column = createNotNullColumn( targetClassName );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getByte() , Byte.MAX_VALUE );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte() , Byte.MIN_VALUE );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getByte() , (byte)-2 );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte() , (byte)-3 );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getByte() , (byte)-4 );
    assertEquals( ( (PrimitiveObject)( column.get(5).getRow() ) ).getByte() , (byte)-5 );
    assertEquals( ( (PrimitiveObject)( column.get(6).getRow() ) ).getByte() , (byte)-6 );
    assertEquals( ( (PrimitiveObject)( column.get(7).getRow() ) ).getByte() , (byte)7 );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getByte() , (byte)8 );
    assertEquals( ( (PrimitiveObject)( column.get(9).getRow() ) ).getByte() , (byte)9 );
    assertEquals( ( (PrimitiveObject)( column.get(10).getRow() ) ).getByte() , (byte)0 );
  }

  @Test( dataProvider = "target_class" )
  public void T_null_1( final String targetClassName ) throws IOException{
    IColumn column = createNullColumn( targetClassName );
    assertNull( column.get(0).getRow() );
    assertNull( column.get(1).getRow() );
  }

  @Test( dataProvider = "target_class" )
  public void T_hasNull_1( final String targetClassName ) throws IOException{
    IColumn column = createHasNullColumn( targetClassName );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getByte() , (byte)0 );
    assertNull( column.get(1).getRow() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getByte() , (byte)4 );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
    assertNull( column.get(7).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getByte() , (byte)8 );
  }

  @Test( dataProvider = "target_class" )
  public void T_lastCell_1( final String targetClassName ) throws IOException{
    IColumn column = createLastCellColumn( targetClassName );
    for( int i = 0 ; i < 10000 ; i++ ){
      assertNull( column.get(i).getRow() );
    }
    assertEquals( ( (PrimitiveObject)( column.get(10000).getRow() ) ).getByte() , Byte.MAX_VALUE );
  }

}
