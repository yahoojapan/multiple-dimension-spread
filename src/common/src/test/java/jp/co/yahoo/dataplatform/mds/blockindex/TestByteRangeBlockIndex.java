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
package jp.co.yahoo.dataplatform.mds.blockindex;

import java.io.IOException;

import org.testng.annotations.Test;
import org.testng.annotations.DataProvider;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

import jp.co.yahoo.dataplatform.schema.objects.*;

import jp.co.yahoo.dataplatform.mds.spread.column.filter.NumberFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.NumberRangeFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.NumberFilterType;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.IFilter;

public class TestByteRangeBlockIndex{

  @DataProvider(name = "T_canBlockSkip_1")
  public Object[][] data1() {
    return new Object[][] {
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ByteObj( (byte)21 ) ) , true },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ByteObj( (byte)9 ) ) , true },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ByteObj( (byte)15 ) ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ByteObj( (byte)10 ) ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ByteObj( (byte)20 ) ) , false },

      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.LT , new ByteObj( (byte)9 ) ) , true },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.LT , new ByteObj( (byte)10 ) ) , true },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.LT , new ByteObj( (byte)11 ) ) , false },

      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.LE , new ByteObj( (byte)9 ) ) , true },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.LE , new ByteObj( (byte)10 ) ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.LE , new ByteObj( (byte)11 ) ) , false },

      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.GT , new ByteObj( (byte)21 ) ) , true },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.GT , new ByteObj( (byte)20 ) ) , true },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.GT , new ByteObj( (byte)19 ) ) , false },

      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.GE , new ByteObj( (byte)21 ) ) , true },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.GE , new ByteObj( (byte)20 ) ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.GE , new ByteObj( (byte)19 ) ) , false },

      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)10 ) , true , new ByteObj( (byte)10 ) , true ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)5 ) , true , new ByteObj( (byte)15 ) , true ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)20 ) , true , new ByteObj( (byte)21 ) , true ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)15 ) , true , new ByteObj( (byte)25 ) , true ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)10 ) , true , new ByteObj( (byte)20 ) , true ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)10 ) , true , new ByteObj( (byte)10 ) , true ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)20 ) , true , new ByteObj( (byte)20 ) , true ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)15 ) , true , new ByteObj( (byte)16 ) , true ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)9 ) , true , new ByteObj( (byte)9 ) , true ) , true },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)21 ) , true , new ByteObj( (byte)21 ) , true ) , true },

      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)10 ) , true , new ByteObj( (byte)10 ) , true ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)5 ) , true , new ByteObj( (byte)15 ) , true ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)20 ) , true , new ByteObj( (byte)21 ) , true ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)15 ) , true , new ByteObj( (byte)25 ) , true ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)10 ) , true , new ByteObj( (byte)20 ) , true ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)10 ) , true , new ByteObj( (byte)10 ) , true ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)20 ) , true , new ByteObj( (byte)20 ) , true ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)15 ) , true , new ByteObj( (byte)16 ) , true ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)9 ) , true , new ByteObj( (byte)9 ) , true ) , false },
      { new ByteRangeBlockIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)21 ) , true , new ByteObj( (byte)21 ) , true ) , false },

    };
  }

  @Test
  public void T_newInstance_1(){
    ByteRangeBlockIndex bIndex = new ByteRangeBlockIndex( (byte)10 , (byte)20 );
    assertEquals( (byte)10 , bIndex.getMin() );
    assertEquals( (byte)20 , bIndex.getMax() );
  }

  @Test
  public void T_newInstance_2(){
    ByteRangeBlockIndex bIndex = new ByteRangeBlockIndex();
    assertEquals( Byte.MAX_VALUE , bIndex.getMin() );
    assertEquals( Byte.MIN_VALUE , bIndex.getMax() );
  }

  @Test
  public void T_getBlockIndexType_1(){
    IBlockIndex bIndex = new ByteRangeBlockIndex();
    assertEquals( BlockIndexType.RANGE_BYTE , bIndex.getBlockIndexType() );
  }

  @Test
  public void T_merge_1(){
    ByteRangeBlockIndex bIndex = new ByteRangeBlockIndex( (byte)10 , (byte)20 );
    assertEquals( (byte)10 , bIndex.getMin() );
    assertEquals( (byte)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new ByteRangeBlockIndex( (byte)110 , (byte)150 ) ) );
    assertEquals( (byte)10 , bIndex.getMin() );
    assertEquals( (byte)20 , bIndex.getMax() );
  }

  @Test
  public void T_merge_2(){
    ByteRangeBlockIndex bIndex = new ByteRangeBlockIndex( (byte)10 , (byte)20 );
    assertEquals( (byte)10 , bIndex.getMin() );
    assertEquals( (byte)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new ByteRangeBlockIndex( (byte)9 , (byte)20 ) ) );
    assertEquals( (byte)9 , bIndex.getMin() );
    assertEquals( (byte)20 , bIndex.getMax() );
  }

  @Test
  public void T_merge_3(){
    ByteRangeBlockIndex bIndex = new ByteRangeBlockIndex( (byte)10 , (byte)20 );
    assertEquals( (byte)10 , bIndex.getMin() );
    assertEquals( (byte)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new ByteRangeBlockIndex( (byte)10 , (byte)21 ) ) );
    assertEquals( (byte)10 , bIndex.getMin() );
    assertEquals( (byte)21 , bIndex.getMax() );
  }

  @Test
  public void T_merge_4(){
    ByteRangeBlockIndex bIndex = new ByteRangeBlockIndex( (byte)10 , (byte)20 );
    assertEquals( (byte)10 , bIndex.getMin() );
    assertEquals( (byte)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new ByteRangeBlockIndex( (byte)9 , (byte)21 ) ) );
    assertEquals( (byte)9 , bIndex.getMin() );
    assertEquals( (byte)21 , bIndex.getMax() );
  }

  @Test
  public void T_merge_5(){
    ByteRangeBlockIndex bIndex = new ByteRangeBlockIndex( (byte)10 , (byte)20 );
    assertEquals( (byte)10 , bIndex.getMin() );
    assertEquals( (byte)20 , bIndex.getMax() );
    assertFalse( bIndex.merge( UnsupportedBlockIndex.INSTANCE ) );
  }

  @Test
  public void T_getBinarySize_1(){
    ByteRangeBlockIndex bIndex = new ByteRangeBlockIndex( (byte)10 , (byte)20 );
    assertEquals( 2 , bIndex.getBinarySize() );
  }

  @Test
  public void T_binary_1(){
    ByteRangeBlockIndex bIndex = new ByteRangeBlockIndex( (byte)10 , (byte)20 );
    byte[] binary = bIndex.toBinary();
    assertEquals( binary.length , bIndex.getBinarySize() );
    ByteRangeBlockIndex bIndex2 = new ByteRangeBlockIndex();
    assertEquals( Byte.MAX_VALUE , bIndex2.getMin() );
    assertEquals( Byte.MIN_VALUE , bIndex2.getMax() );
    bIndex2.setFromBinary( binary , 0 , binary.length );
    assertEquals( bIndex2.getMin() , bIndex.getMin() );
    assertEquals( bIndex2.getMax() , bIndex.getMax() );
  }

  @Test( dataProvider = "T_canBlockSkip_1" )
  public void T_canBlockSkip_1( final IBlockIndex bIndex , final IFilter filter , final boolean result ){
    if( result ){
      assertEquals( result , bIndex.getBlockSpreadIndex( filter ).isEmpty() );
    }
    else{
      assertTrue( bIndex.getBlockSpreadIndex( filter ) == null );
    }
  }

}
