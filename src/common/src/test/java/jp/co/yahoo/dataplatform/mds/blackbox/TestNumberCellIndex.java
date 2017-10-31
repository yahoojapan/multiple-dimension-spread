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

import java.util.Set;
import java.util.HashSet;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

import jp.co.yahoo.dataplatform.config.Configuration;

import jp.co.yahoo.dataplatform.schema.objects.*;

import jp.co.yahoo.dataplatform.mds.spread.expression.*;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.*;
import jp.co.yahoo.dataplatform.mds.spread.column.*;
import jp.co.yahoo.dataplatform.mds.binary.*;
import jp.co.yahoo.dataplatform.mds.binary.maker.*;

public class TestNumberCellIndex{

  @DataProvider(name = "target_class")
  public Object[][] data1() throws IOException{
    return new Object[][] {
      { createByteTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeLongColumnBinaryMaker" ) },
      { createByteTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeDumpLongColumnBinaryMaker" ) },

      { createShortTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeLongColumnBinaryMaker" ) },
      { createShortTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeDumpLongColumnBinaryMaker" ) },

      { createIntTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeLongColumnBinaryMaker" ) },
      { createIntTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeDumpLongColumnBinaryMaker" ) },

      { createLongTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeLongColumnBinaryMaker" ) },
      { createLongTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeDumpLongColumnBinaryMaker" ) },

      { createFloatTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpFloatColumnBinaryMaker" ) },
      { createFloatTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.RangeDumpFloatColumnBinaryMaker" ) },
      { createFloatTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeFloatColumnBinaryMaker" ) },

      { createDoubleTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpDoubleColumnBinaryMaker" ) },
      { createDoubleTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.RangeDumpDoubleColumnBinaryMaker" ) },
      { createDoubleTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeDoubleColumnBinaryMaker" ) },

      { createStringTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeDumpStringColumnBinaryMaker" ) },
      { createBytesTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpBytesColumnBinaryMaker" ) },
    };
  }

  public IColumn createByteTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BYTE , "column" );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-10 ) , 0 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-11 ) , 1 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-12 ) , 2 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-13 ) , 3 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-14 ) , 4 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-15 ) , 5 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-16 ) , 6 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-17 ) , 7 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-18 ) , 8 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-19 ) , 9 );

    column.add( ColumnType.BYTE , new ByteObj( (byte)20 ) , 20 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)21 ) , 21 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)22 ) , 22 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)23 ) , 23 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)24 ) , 24 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)25 ) , 25 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)26 ) , 26 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)27 ) , 27 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)28 ) , 28 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)29 ) , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );
    return maker.toColumn( columnBinary );
  }

  public IColumn createShortTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.SHORT , "column" );
    column.add( ColumnType.SHORT , new ShortObj( (short)-10 ) , 0 );
    column.add( ColumnType.SHORT , new ShortObj( (short)-11 ) , 1 );
    column.add( ColumnType.SHORT , new ShortObj( (short)-12 ) , 2 );
    column.add( ColumnType.SHORT , new ShortObj( (short)-13 ) , 3 );
    column.add( ColumnType.SHORT , new ShortObj( (short)-14 ) , 4 );
    column.add( ColumnType.SHORT , new ShortObj( (short)-15 ) , 5 );
    column.add( ColumnType.SHORT , new ShortObj( (short)-16 ) , 6 );
    column.add( ColumnType.SHORT , new ShortObj( (short)-17 ) , 7 );
    column.add( ColumnType.SHORT , new ShortObj( (short)-18 ) , 8 );
    column.add( ColumnType.SHORT , new ShortObj( (short)-19 ) , 9 );

    column.add( ColumnType.SHORT , new ShortObj( (short)20 ) , 20 );
    column.add( ColumnType.SHORT , new ShortObj( (short)21 ) , 21 );
    column.add( ColumnType.SHORT , new ShortObj( (short)22 ) , 22 );
    column.add( ColumnType.SHORT , new ShortObj( (short)23 ) , 23 );
    column.add( ColumnType.SHORT , new ShortObj( (short)24 ) , 24 );
    column.add( ColumnType.SHORT , new ShortObj( (short)25 ) , 25 );
    column.add( ColumnType.SHORT , new ShortObj( (short)26 ) , 26 );
    column.add( ColumnType.SHORT , new ShortObj( (short)27 ) , 27 );
    column.add( ColumnType.SHORT , new ShortObj( (short)28 ) , 28 );
    column.add( ColumnType.SHORT , new ShortObj( (short)29 ) , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );
    return maker.toColumn( columnBinary );
  }

  public IColumn createIntTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.INTEGER , "column" );
    column.add( ColumnType.INTEGER , new IntegerObj( -10 ) , 0 );
    column.add( ColumnType.INTEGER , new IntegerObj( -11 ) , 1 );
    column.add( ColumnType.INTEGER , new IntegerObj( -12 ) , 2 );
    column.add( ColumnType.INTEGER , new IntegerObj( -13 ) , 3 );
    column.add( ColumnType.INTEGER , new IntegerObj( -14 ) , 4 );
    column.add( ColumnType.INTEGER , new IntegerObj( -15 ) , 5 );
    column.add( ColumnType.INTEGER , new IntegerObj( -16 ) , 6 );
    column.add( ColumnType.INTEGER , new IntegerObj( -17 ) , 7 );
    column.add( ColumnType.INTEGER , new IntegerObj( -18 ) , 8 );
    column.add( ColumnType.INTEGER , new IntegerObj( -19 ) , 9 );

    column.add( ColumnType.INTEGER , new IntegerObj( 20 ) , 20 );
    column.add( ColumnType.INTEGER , new IntegerObj( 21 ) , 21 );
    column.add( ColumnType.INTEGER , new IntegerObj( 22 ) , 22 );
    column.add( ColumnType.INTEGER , new IntegerObj( 23 ) , 23 );
    column.add( ColumnType.INTEGER , new IntegerObj( 24 ) , 24 );
    column.add( ColumnType.INTEGER , new IntegerObj( 25 ) , 25 );
    column.add( ColumnType.INTEGER , new IntegerObj( 26 ) , 26 );
    column.add( ColumnType.INTEGER , new IntegerObj( 27 ) , 27 );
    column.add( ColumnType.INTEGER , new IntegerObj( 28 ) , 28 );
    column.add( ColumnType.INTEGER , new IntegerObj( 29 ) , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );
    return maker.toColumn( columnBinary );
  }

  public IColumn createLongTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.LONG , "column" );
    column.add( ColumnType.LONG , new LongObj( -10 ) , 0 );
    column.add( ColumnType.LONG , new LongObj( -11 ) , 1 );
    column.add( ColumnType.LONG , new LongObj( -12 ) , 2 );
    column.add( ColumnType.LONG , new LongObj( -13 ) , 3 );
    column.add( ColumnType.LONG , new LongObj( -14 ) , 4 );
    column.add( ColumnType.LONG , new LongObj( -15 ) , 5 );
    column.add( ColumnType.LONG , new LongObj( -16 ) , 6 );
    column.add( ColumnType.LONG , new LongObj( -17 ) , 7 );
    column.add( ColumnType.LONG , new LongObj( -18 ) , 8 );
    column.add( ColumnType.LONG , new LongObj( -19 ) , 9 );

    column.add( ColumnType.LONG , new LongObj( 20 ) , 20 );
    column.add( ColumnType.LONG , new LongObj( 21 ) , 21 );
    column.add( ColumnType.LONG , new LongObj( 22 ) , 22 );
    column.add( ColumnType.LONG , new LongObj( 23 ) , 23 );
    column.add( ColumnType.LONG , new LongObj( 24 ) , 24 );
    column.add( ColumnType.LONG , new LongObj( 25 ) , 25 );
    column.add( ColumnType.LONG , new LongObj( 26 ) , 26 );
    column.add( ColumnType.LONG , new LongObj( 27 ) , 27 );
    column.add( ColumnType.LONG , new LongObj( 28 ) , 28 );
    column.add( ColumnType.LONG , new LongObj( 29 ) , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );
    return maker.toColumn( columnBinary );
  }

  public IColumn createFloatTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.FLOAT , "column" );
    column.add( ColumnType.FLOAT , new FloatObj( -10.0f ) , 0 );
    column.add( ColumnType.FLOAT , new FloatObj( -11.0f ) , 1 );
    column.add( ColumnType.FLOAT , new FloatObj( -12.0f ) , 2 );
    column.add( ColumnType.FLOAT , new FloatObj( -13.0f ) , 3 );
    column.add( ColumnType.FLOAT , new FloatObj( -14.0f ) , 4 );
    column.add( ColumnType.FLOAT , new FloatObj( -15.0f ) , 5 );
    column.add( ColumnType.FLOAT , new FloatObj( -16.0f ) , 6 );
    column.add( ColumnType.FLOAT , new FloatObj( -17.0f ) , 7 );
    column.add( ColumnType.FLOAT , new FloatObj( -18.0f ) , 8 );
    column.add( ColumnType.FLOAT , new FloatObj( -19.0f ) , 9 );

    column.add( ColumnType.FLOAT , new FloatObj( 20.0f ) , 20 );
    column.add( ColumnType.FLOAT , new FloatObj( 21.0f ) , 21 );
    column.add( ColumnType.FLOAT , new FloatObj( 22.0f ) , 22 );
    column.add( ColumnType.FLOAT , new FloatObj( 23.0f ) , 23 );
    column.add( ColumnType.FLOAT , new FloatObj( 24.0f ) , 24 );
    column.add( ColumnType.FLOAT , new FloatObj( 25.0f ) , 25 );
    column.add( ColumnType.FLOAT , new FloatObj( 26.0f ) , 26 );
    column.add( ColumnType.FLOAT , new FloatObj( 27.0f ) , 27 );
    column.add( ColumnType.FLOAT , new FloatObj( 28.0f ) , 28 );
    column.add( ColumnType.FLOAT , new FloatObj( 29.0f ) , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );
    return maker.toColumn( columnBinary );
  }

  public IColumn createDoubleTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.DOUBLE , "column" );
    column.add( ColumnType.DOUBLE , new DoubleObj( -10.0d ) , 0 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -11.0d ) , 1 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -12.0d ) , 2 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -13.0d ) , 3 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -14.0d ) , 4 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -15.0d ) , 5 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -16.0d ) , 6 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -17.0d ) , 7 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -18.0d ) , 8 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -19.0d ) , 9 );

    column.add( ColumnType.DOUBLE , new DoubleObj( 20.0d ) , 20 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 21.0d ) , 21 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 22.0d ) , 22 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 23.0d ) , 23 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 24.0d ) , 24 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 25.0d ) , 25 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 26.0d ) , 26 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 27.0d ) , 27 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 28.0d ) , 28 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 29.0d ) , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );
    return maker.toColumn( columnBinary );
  }

  public IColumn createStringTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column" );
    column.add( ColumnType.STRING , new StringObj( "-10" ) , 0 );
    column.add( ColumnType.STRING , new StringObj( "-11" ) , 1 );
    column.add( ColumnType.STRING , new StringObj( "-12" ) , 2 );
    column.add( ColumnType.STRING , new StringObj( "-13" ) , 3 );
    column.add( ColumnType.STRING , new StringObj( "-14" ) , 4 );
    column.add( ColumnType.STRING , new StringObj( "-15" ) , 5 );
    column.add( ColumnType.STRING , new StringObj( "-16" ) , 6 );
    column.add( ColumnType.STRING , new StringObj( "-17" ) , 7 );
    column.add( ColumnType.STRING , new StringObj( "-18" ) , 8 );
    column.add( ColumnType.STRING , new StringObj( "-19" ) , 9 );

    column.add( ColumnType.STRING , new StringObj( "20" ) , 20 );
    column.add( ColumnType.STRING , new StringObj( "21" ) , 21 );
    column.add( ColumnType.STRING , new StringObj( "22" ) , 22 );
    column.add( ColumnType.STRING , new StringObj( "23" ) , 23 );
    column.add( ColumnType.STRING , new StringObj( "24" ) , 24 );
    column.add( ColumnType.STRING , new StringObj( "25" ) , 25 );
    column.add( ColumnType.STRING , new StringObj( "26" ) , 26 );
    column.add( ColumnType.STRING , new StringObj( "27" ) , 27 );
    column.add( ColumnType.STRING , new StringObj( "28" ) , 28 );
    column.add( ColumnType.STRING , new StringObj( "29" ) , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );
    return maker.toColumn( columnBinary );
  }

  public IColumn createBytesTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BYTES , "column" );
    column.add( ColumnType.BYTES , new BytesObj( "-10".getBytes() ) , 0 );
    column.add( ColumnType.BYTES , new BytesObj( "-11".getBytes() ) , 1 );
    column.add( ColumnType.BYTES , new BytesObj( "-12".getBytes() ) , 2 );
    column.add( ColumnType.BYTES , new BytesObj( "-13".getBytes() ) , 3 );
    column.add( ColumnType.BYTES , new BytesObj( "-14".getBytes() ) , 4 );
    column.add( ColumnType.BYTES , new BytesObj( "-15".getBytes() ) , 5 );
    column.add( ColumnType.BYTES , new BytesObj( "-16".getBytes() ) , 6 );
    column.add( ColumnType.BYTES , new BytesObj( "-17".getBytes() ) , 7 );
    column.add( ColumnType.BYTES , new BytesObj( "-18".getBytes() ) , 8 );
    column.add( ColumnType.BYTES , new BytesObj( "-19".getBytes() ) , 9 );

    column.add( ColumnType.BYTES , new BytesObj( "20".getBytes() ) , 20 );
    column.add( ColumnType.BYTES , new BytesObj( "21".getBytes() ) , 21 );
    column.add( ColumnType.BYTES , new BytesObj( "22".getBytes() ) , 22 );
    column.add( ColumnType.BYTES , new BytesObj( "23".getBytes() ) , 23 );
    column.add( ColumnType.BYTES , new BytesObj( "24".getBytes() ) , 24 );
    column.add( ColumnType.BYTES , new BytesObj( "25".getBytes() ) , 25 );
    column.add( ColumnType.BYTES , new BytesObj( "26".getBytes() ) , 26 );
    column.add( ColumnType.BYTES , new BytesObj( "27".getBytes() ) , 27 );
    column.add( ColumnType.BYTES , new BytesObj( "28".getBytes() ) , 28 );
    column.add( ColumnType.BYTES , new BytesObj( "29".getBytes() ) , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );
    return maker.toColumn( columnBinary );
  }

  public void dumpFilterResult( final boolean[] result ){
    System.out.println( "-----------------------" );
    System.out.println( "Integer cell index test result." );
    System.out.println( "-----------------------" );
    for( int i = 0 ; i < result.length ; i++ ){
      System.out.println( String.format( "index:%d = %s" , i , Boolean.toString( result[i] ) ) );
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_equal_obj_1( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 };
    PrimitiveObject[] compareData = new PrimitiveObject[]{ new ByteObj( (byte)-10 ) , new ShortObj( (short)-10 ) , new IntegerObj( -10 ) , new LongObj( -10 ) , new FloatObj( -10.0f ) , new DoubleObj( -10.0d ) };
    for( PrimitiveObject obj : compareData ){
      IFilter filter = new NumberFilter( NumberFilterType.EQUAL , obj );
      boolean[] filterResult = new boolean[30];
      filterResult = column.filter( filter , filterResult );
      if( filterResult == null ){
        assertTrue( true );
        return;
      }
      for( int i = 0 ; i < mustReadIndex.length ; i++ ){
        assertTrue( filterResult[mustReadIndex[i]] );
      }
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_equal_obj_2( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 22 };
    PrimitiveObject[] compareData = new PrimitiveObject[]{ new ByteObj( (byte)22 ) , new ShortObj( (short)22 ) , new IntegerObj( 22 ) , new LongObj( 22 ) , new FloatObj( 22.0f ) , new DoubleObj( 22.0d ) };
    for( PrimitiveObject obj : compareData ){
      IFilter filter = new NumberFilter( NumberFilterType.EQUAL , obj );
      boolean[] filterResult = new boolean[30];
      filterResult = column.filter( filter , filterResult );
      if( filterResult == null ){
        assertTrue( true );
        return;
      }
      for( int i = 0 ; i < mustReadIndex.length ; i++ ){
        assertTrue( filterResult[mustReadIndex[i]] );
      }
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_equal_obj_3( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 29 };
    PrimitiveObject[] compareData = new PrimitiveObject[]{ new ByteObj( (byte)29 ) , new ShortObj( (short)29 ) , new IntegerObj( 29 ) , new LongObj( 29 ) , new FloatObj( 29.0f ) , new DoubleObj( 29.0d ) };
    for( PrimitiveObject obj : compareData ){
      IFilter filter = new NumberFilter( NumberFilterType.EQUAL , obj );
      boolean[] filterResult = new boolean[30];
      filterResult = column.filter( filter , filterResult );
      if( filterResult == null ){
        assertTrue( true );
        return;
      }
      for( int i = 0 ; i < mustReadIndex.length ; i++ ){
        assertTrue( filterResult[mustReadIndex[i]] );
      }
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_notequal_obj_1( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14 , 15 , 16 , 17 , 18 , 19 , 20 , 21 , 22 , 23 , 24 , 25 , 26 , 27 , 28 , 29 };
    PrimitiveObject[] compareData = new PrimitiveObject[]{ new ByteObj( (byte)-10 ) , new ShortObj( (short)-10 ) , new IntegerObj( -10 ) , new LongObj( -10 ) , new FloatObj( -10.0f ) , new DoubleObj( -10.0d ) };
    for( PrimitiveObject obj : compareData ){
      IFilter filter = new NumberFilter( NumberFilterType.NOT_EQUAL , obj );
      boolean[] filterResult = new boolean[30];
      filterResult = column.filter( filter , filterResult );
      if( filterResult == null ){
        assertTrue( true );
        return;
      }
      for( int i = 0 ; i < mustReadIndex.length ; i++ ){
        assertTrue( filterResult[mustReadIndex[i]] );
      }
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_notequal_obj_2( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14 , 15 , 16 , 17 , 18 , 19 , 20 , 21 , 23 , 24 , 25 , 26 , 27 , 28 , 29 };
    PrimitiveObject[] compareData = new PrimitiveObject[]{ new ByteObj( (byte)22 ) , new ShortObj( (short)22 ) , new IntegerObj( 22 ) , new LongObj( 22 ) , new FloatObj( 22.0f ) , new DoubleObj( 22.0d ) };
    for( PrimitiveObject obj : compareData ){
      IFilter filter = new NumberFilter( NumberFilterType.NOT_EQUAL , obj );
      boolean[] filterResult = new boolean[30];
      filterResult = column.filter( filter , filterResult );
      if( filterResult == null ){
        assertTrue( true );
        return;
      }
      for( int i = 0 ; i < mustReadIndex.length ; i++ ){
        assertTrue( filterResult[mustReadIndex[i]] );
      }
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_notequal_obj_3( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14 , 15 , 16 , 17 , 18 , 19 , 20 , 21 , 22 , 23 , 24 , 25 , 26 , 27 , 28 };
    PrimitiveObject[] compareData = new PrimitiveObject[]{ new ByteObj( (byte)29 ) , new ShortObj( (short)29 ) , new IntegerObj( 29 ) , new LongObj( 29 ) , new FloatObj( 29.0f ) , new DoubleObj( 29.0d ) };
    for( PrimitiveObject obj : compareData ){
      IFilter filter = new NumberFilter( NumberFilterType.NOT_EQUAL , obj );
      boolean[] filterResult = new boolean[30];
      filterResult = column.filter( filter , filterResult );
      if( filterResult == null ){
        assertTrue( true );
        return;
      }
      //dumpFilterResult( filterResult );
      for( int i = 0 ; i < mustReadIndex.length ; i++ ){
        assertTrue( filterResult[mustReadIndex[i]] );
      }
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_lt_obj_1( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 9 };
    PrimitiveObject[] compareData = new PrimitiveObject[]{ new ByteObj( (byte)-18 ) , new ShortObj( (short)-18 ) , new IntegerObj( -18 ) , new LongObj( -18 ) , new FloatObj( -18.0f ) , new DoubleObj( -18.0d ) };
    for( PrimitiveObject obj : compareData ){
      IFilter filter = new NumberFilter( NumberFilterType.LT , obj );
      boolean[] filterResult = new boolean[30];
      filterResult = column.filter( filter , filterResult );
      if( filterResult == null ){
        assertTrue( true );
        return;
      }
      for( int i = 0 ; i < mustReadIndex.length ; i++ ){
        assertTrue( filterResult[mustReadIndex[i]] );
      }
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_lt_obj_2( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 20 , 21 , 22 , 23 , 24 , 25 , 26 , 27 , 28 };
    PrimitiveObject[] compareData = new PrimitiveObject[]{ new ByteObj( (byte)29 ) , new ShortObj( (short)29 ) , new IntegerObj( 29 ) , new LongObj( 29 ) , new FloatObj( 29.0f ) , new DoubleObj( 29.0d ) };
    for( PrimitiveObject obj : compareData ){
      IFilter filter = new NumberFilter( NumberFilterType.LT , obj );
      boolean[] filterResult = new boolean[30];
      filterResult = column.filter( filter , filterResult );
      if( filterResult == null ){
        assertTrue( true );
        return;
      }
      for( int i = 0 ; i < mustReadIndex.length ; i++ ){
        assertTrue( filterResult[mustReadIndex[i]] );
      }
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_lt_obj_3( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 };
    PrimitiveObject[] compareData = new PrimitiveObject[]{ new ByteObj( (byte)0 ) , new ShortObj( (short)0 ) , new IntegerObj( 0 ) , new LongObj( 0 ) , new FloatObj( 0.0f ) , new DoubleObj( 0.0d ) };
    for( PrimitiveObject obj : compareData ){
      IFilter filter = new NumberFilter( NumberFilterType.LT , obj );
      boolean[] filterResult = new boolean[30];
      filterResult = column.filter( filter , filterResult );
      if( filterResult == null ){
        assertTrue( true );
        return;
      }
      //dumpFilterResult( filterResult );
      for( int i = 0 ; i < mustReadIndex.length ; i++ ){
        assertTrue( filterResult[mustReadIndex[i]] );
      }
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_le_obj_1( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 8 , 9 };
    PrimitiveObject[] compareData = new PrimitiveObject[]{ new ByteObj( (byte)-18 ) , new ShortObj( (short)-18 ) , new IntegerObj( -18 ) , new LongObj( -18 ) , new FloatObj( -18.0f ) , new DoubleObj( -18.0d ) };
    for( PrimitiveObject obj : compareData ){
      IFilter filter = new NumberFilter( NumberFilterType.LE , obj );
      boolean[] filterResult = new boolean[30];
      filterResult = column.filter( filter , filterResult );
      if( filterResult == null ){
        assertTrue( true );
        return;
      }
      for( int i = 0 ; i < mustReadIndex.length ; i++ ){
        assertTrue( filterResult[mustReadIndex[i]] );
      }
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_le_obj_2( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 20 , 21 , 22 , 23 , 24 , 25 , 26 , 27 , 28 , 29 };
    PrimitiveObject[] compareData = new PrimitiveObject[]{ new ByteObj( (byte)29 ) , new ShortObj( (short)29 ) , new IntegerObj( 29 ) , new LongObj( 29 ) , new FloatObj( 29.0f ) , new DoubleObj( 29.0d ) };
    for( PrimitiveObject obj : compareData ){
      IFilter filter = new NumberFilter( NumberFilterType.LE , obj );
      boolean[] filterResult = new boolean[30];
      filterResult = column.filter( filter , filterResult );
      if( filterResult == null ){
        assertTrue( true );
        return;
      }
      for( int i = 0 ; i < mustReadIndex.length ; i++ ){
        assertTrue( filterResult[mustReadIndex[i]] );
      }
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_le_obj_3( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 };
    PrimitiveObject[] compareData = new PrimitiveObject[]{ new ByteObj( (byte)0 ) , new ShortObj( (short)0 ) , new IntegerObj( 0 ) , new LongObj( 0 ) , new FloatObj( 0.0f ) , new DoubleObj( 0.0d ) };
    for( PrimitiveObject obj : compareData ){
      IFilter filter = new NumberFilter( NumberFilterType.LE , obj );
      boolean[] filterResult = new boolean[30];
      filterResult = column.filter( filter , filterResult );
      if( filterResult == null ){
        assertTrue( true );
        return;
      }
      //dumpFilterResult( filterResult );
      for( int i = 0 ; i < mustReadIndex.length ; i++ ){
        assertTrue( filterResult[mustReadIndex[i]] );
      }
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_gt_obj_1( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 20 , 21 , 22 , 23 , 24 , 25 , 26 , 27 , 28 , 29 };
    PrimitiveObject[] compareData = new PrimitiveObject[]{ new ByteObj( (byte)-19 ) , new ShortObj( (short)-19 ) , new IntegerObj( -19 ) , new LongObj( -19 ) , new FloatObj( -19.0f ) , new DoubleObj( -19.0d ) };
    for( PrimitiveObject obj : compareData ){
      IFilter filter = new NumberFilter( NumberFilterType.GT , obj );
      boolean[] filterResult = new boolean[30];
      filterResult = column.filter( filter , filterResult );
      if( filterResult == null ){
        assertTrue( true );
        return;
      }
      for( int i = 0 ; i < mustReadIndex.length ; i++ ){
        assertTrue( filterResult[mustReadIndex[i]] );
      }
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_gt_obj_2( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 29 };
    PrimitiveObject[] compareData = new PrimitiveObject[]{ new ByteObj( (byte)28 ) , new ShortObj( (short)28 ) , new IntegerObj( 28 ) , new LongObj( 28 ) , new FloatObj( 28.0f ) , new DoubleObj( 28.0d ) };
    for( PrimitiveObject obj : compareData ){
      IFilter filter = new NumberFilter( NumberFilterType.GT , obj );
      boolean[] filterResult = new boolean[30];
      filterResult = column.filter( filter , filterResult );
      if( filterResult == null ){
        assertTrue( true );
        return;
      }
      for( int i = 0 ; i < mustReadIndex.length ; i++ ){
        assertTrue( filterResult[mustReadIndex[i]] );
      }
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_gt_obj_3( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 20 , 21 , 22 , 23 , 24 , 25 , 26 , 27 , 28 , 29 };
    PrimitiveObject[] compareData = new PrimitiveObject[]{ new ByteObj( (byte)0 ) , new ShortObj( (short)0 ) , new IntegerObj( 0 ) , new LongObj( 0 ) , new FloatObj( 0.0f ) , new DoubleObj( 0.0d ) };
    for( PrimitiveObject obj : compareData ){
      IFilter filter = new NumberFilter( NumberFilterType.GT , obj );
      boolean[] filterResult = new boolean[30];
      filterResult = column.filter( filter , filterResult );
      if( filterResult == null ){
        assertTrue( true );
        return;
      }
      //dumpFilterResult( filterResult );
      for( int i = 0 ; i < mustReadIndex.length ; i++ ){
        assertTrue( filterResult[mustReadIndex[i]] );
      }
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_ge_obj_1( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 20 , 21 , 22 , 23 , 24 , 25 , 26 , 27 , 28 , 29 };
    PrimitiveObject[] compareData = new PrimitiveObject[]{ new ByteObj( (byte)-19 ) , new ShortObj( (short)-19 ) , new IntegerObj( -19 ) , new LongObj( -19 ) , new FloatObj( -19.0f ) , new DoubleObj( -19.0d ) };
    for( PrimitiveObject obj : compareData ){
      IFilter filter = new NumberFilter( NumberFilterType.GE , obj );
      boolean[] filterResult = new boolean[30];
      filterResult = column.filter( filter , filterResult );
      if( filterResult == null ){
        assertTrue( true );
        return ;
      }
      for( int i = 0 ; i < mustReadIndex.length ; i++ ){
        assertTrue( filterResult[mustReadIndex[i]] );
      }
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_ge_obj_2( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 28 , 29 };
    PrimitiveObject[] compareData = new PrimitiveObject[]{ new ByteObj( (byte)28 ) , new ShortObj( (short)28 ) , new IntegerObj( 28 ) , new LongObj( 28 ) , new FloatObj( 28.0f ) , new DoubleObj( 28.0d ) };
    for( PrimitiveObject obj : compareData ){
      IFilter filter = new NumberFilter( NumberFilterType.GE , obj );
      boolean[] filterResult = new boolean[30];
      filterResult = column.filter( filter , filterResult );
      if( filterResult == null ){
        assertTrue( true );
        return;
      }
      for( int i = 0 ; i < mustReadIndex.length ; i++ ){
        assertTrue( filterResult[mustReadIndex[i]] );
      }
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_ge_obj_3( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 20 , 21 , 22 , 23 , 24 , 25 , 26 , 27 , 28 , 29 };
    PrimitiveObject[] compareData = new PrimitiveObject[]{ new ByteObj( (byte)0 ) , new ShortObj( (short)0 ) , new IntegerObj( 0 ) , new LongObj( 0 ) , new FloatObj( 0.0f ) , new DoubleObj( 0.0d ) };
    for( PrimitiveObject obj : compareData ){
      IFilter filter = new NumberFilter( NumberFilterType.GE , obj );
      boolean[] filterResult = new boolean[30];
      filterResult = column.filter( filter , filterResult );
      if( filterResult == null ){
        assertTrue( true );
        return;
      }
      //dumpFilterResult( filterResult );
      for( int i = 0 ; i < mustReadIndex.length ; i++ ){
        assertTrue( filterResult[mustReadIndex[i]] );
      }
    }
  }

}
