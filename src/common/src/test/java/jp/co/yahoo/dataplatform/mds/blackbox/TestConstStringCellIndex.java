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

public class TestConstStringCellIndex{

  @DataProvider(name = "target_class")
  public Object[][] data1() throws IOException{
    return new Object[][] {
      { createConstTestData( new ByteObj( (byte)15 ) ) },
      { createConstTestData( new ShortObj( (short)15 ) ) },
      { createConstTestData( new IntegerObj( 15 ) ) },
      { createConstTestData( new LongObj( 15 ) ) },
      { createConstTestData( new FloatObj( 15.0f ) ) },
      { createConstTestData( new DoubleObj( 15.0d ) ) },

      { createConstTestData( new StringObj( "15" ) ) },
      { createConstTestData( new BytesObj( "15".getBytes() ) ) },
    };
  }

  private IColumn createConstTestData( final PrimitiveObject value ) throws IOException{
    ConstantColumnBinaryMaker maker = new ConstantColumnBinaryMaker();
    ColumnBinary columnBinary = ConstantColumnBinaryMaker.createColumnBinary( value , "t" , 10 );
    return maker.toColumn( columnBinary );
  }

  public void dumpFilterResult( final boolean[] result ){
    System.out.println( "-----------------------" );
    System.out.println( "String cell index test result." );
    System.out.println( "-----------------------" );
    for( int i = 0 ; i < result.length ; i++ ){
      System.out.println( String.format( "index:%d = %s" , i , Boolean.toString( result[i] ) ) );
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_perfectMatch_1( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 };
    IFilter filter = new PerfectMatchStringFilter( "15" );
    boolean[] filterResult = new boolean[10];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

  @Test( dataProvider = "target_class" )
  public void T_partialMatch_1( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 };
    IFilter filter = new PartialMatchStringFilter( "1" );
    boolean[] filterResult = new boolean[10];
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

  @Test( dataProvider = "target_class" )
  public void T_partialMatch_2( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 };
    IFilter filter = new PartialMatchStringFilter( "15" );
    boolean[] filterResult = new boolean[10];
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

  @Test( dataProvider = "target_class" )
  public void T_forwardMatch_1( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 };
    IFilter filter = new ForwardMatchStringFilter( "1" );
    boolean[] filterResult = new boolean[10];
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

  @Test( dataProvider = "target_class" )
  public void T_forwardMatch_2( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 };
    IFilter filter = new ForwardMatchStringFilter( "15" );
    boolean[] filterResult = new boolean[10];
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

  @Test( dataProvider = "target_class" )
  public void T_backwardMatch_1( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 };
    IFilter filter = new BackwardMatchStringFilter( "5" );
    boolean[] filterResult = new boolean[10];
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

  @Test( dataProvider = "target_class" )
  public void T_backwardMatch_2( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 };
    IFilter filter = new BackwardMatchStringFilter( "15" );
    boolean[] filterResult = new boolean[10];
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

  @Test( dataProvider = "target_class" )
  public void T_compareString_1( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 };
    IFilter filter = new RangeStringCompareFilter( "15" , true , "15" , true );
    boolean[] filterResult = new boolean[10];
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

  @Test( dataProvider = "target_class" )
  public void T_compareString_2( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 };
    IFilter filter = new RangeStringCompareFilter( "1" , false , "15" , true );
    boolean[] filterResult = new boolean[10];
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

  @Test( dataProvider = "target_class" )
  public void T_compareString_3( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 };
    IFilter filter = new RangeStringCompareFilter( "15" , true , "16" , false );
    boolean[] filterResult = new boolean[10];
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

  @Test( dataProvider = "target_class" )
  public void T_compareString_4( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 };
    IFilter filter = new RangeStringCompareFilter( "1" , false , "16" , false );
    boolean[] filterResult = new boolean[10];
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

  @Test( dataProvider = "target_class" )
  public void T_compareString_5( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 };
    IFilter filter = new RangeStringCompareFilter( "0" , true , "14" , true , true );
    boolean[] filterResult = new boolean[10];
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

  @Test( dataProvider = "target_class" )
  public void T_compareString_6( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 };
    IFilter filter = new RangeStringCompareFilter( "15" , false , "2" , true , true );
    boolean[] filterResult = new boolean[10];
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

  @Test( dataProvider = "target_class" )
  public void T_compareString_7( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 };
    IFilter filter = new RangeStringCompareFilter( "0" , true , "15" , false , true );
    boolean[] filterResult = new boolean[10];
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

  @Test( dataProvider = "target_class" )
  public void T_compareString_8( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 };
    IFilter filter = new RangeStringCompareFilter( "05" , false , "15" , false , true );
    boolean[] filterResult = new boolean[10];
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

  @Test( dataProvider = "target_class" )
  public void T_dictionaryString_1( final IColumn column ) throws IOException{
    Set<String> dic = new HashSet<String>();
    dic.add( "15" );
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 };
    IFilter filter = new StringDictionaryFilter( dic );
    boolean[] filterResult = new boolean[10];
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
