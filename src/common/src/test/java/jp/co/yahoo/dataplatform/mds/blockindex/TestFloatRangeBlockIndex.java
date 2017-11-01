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

public class TestFloatRangeBlockIndex{

  @DataProvider(name = "T_canBlockSkip_1")
  public Object[][] data1() {
    return new Object[][] {
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberFilter( NumberFilterType.EQUAL , new FloatObj( (float)21 ) ) , true },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberFilter( NumberFilterType.EQUAL , new FloatObj( (float)9 ) ) , true },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberFilter( NumberFilterType.EQUAL , new FloatObj( (float)15 ) ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberFilter( NumberFilterType.EQUAL , new FloatObj( (float)10 ) ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberFilter( NumberFilterType.EQUAL , new FloatObj( (float)20 ) ) , false },

      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberFilter( NumberFilterType.LT , new FloatObj( (float)9 ) ) , true },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberFilter( NumberFilterType.LT , new FloatObj( (float)10 ) ) , true },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberFilter( NumberFilterType.LT , new FloatObj( (float)11 ) ) , false },

      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberFilter( NumberFilterType.LE , new FloatObj( (float)9 ) ) , true },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberFilter( NumberFilterType.LE , new FloatObj( (float)10 ) ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberFilter( NumberFilterType.LE , new FloatObj( (float)11 ) ) , false },

      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberFilter( NumberFilterType.GT , new FloatObj( (float)21 ) ) , true },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberFilter( NumberFilterType.GT , new FloatObj( (float)20 ) ) , true },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberFilter( NumberFilterType.GT , new FloatObj( (float)19 ) ) , false },

      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberFilter( NumberFilterType.GE , new FloatObj( (float)21 ) ) , true },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberFilter( NumberFilterType.GE , new FloatObj( (float)20 ) ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberFilter( NumberFilterType.GE , new FloatObj( (float)19 ) ) , false },

      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( false , new FloatObj( (float)10 ) , true , new FloatObj( (float)10 ) , true ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( false , new FloatObj( (float)5 ) , true , new FloatObj( (float)15 ) , true ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( false , new FloatObj( (float)20 ) , true , new FloatObj( (float)21 ) , true ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( false , new FloatObj( (float)15 ) , true , new FloatObj( (float)25 ) , true ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( false , new FloatObj( (float)10 ) , true , new FloatObj( (float)20 ) , true ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( false , new FloatObj( (float)10 ) , true , new FloatObj( (float)10 ) , true ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( false , new FloatObj( (float)20 ) , true , new FloatObj( (float)20 ) , true ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( false , new FloatObj( (float)15 ) , true , new FloatObj( (float)16 ) , true ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( false , new FloatObj( (float)9 ) , true , new FloatObj( (float)9 ) , true ) , true },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( false , new FloatObj( (float)21 ) , true , new FloatObj( (float)21 ) , true ) , true },

      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( true , new FloatObj( (float)10 ) , true , new FloatObj( (float)10 ) , true ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( true , new FloatObj( (float)5 ) , true , new FloatObj( (float)15 ) , true ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( true , new FloatObj( (float)20 ) , true , new FloatObj( (float)21 ) , true ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( true , new FloatObj( (float)15 ) , true , new FloatObj( (float)25 ) , true ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( true , new FloatObj( (float)10 ) , true , new FloatObj( (float)20 ) , true ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( true , new FloatObj( (float)10 ) , true , new FloatObj( (float)10 ) , true ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( true , new FloatObj( (float)20 ) , true , new FloatObj( (float)20 ) , true ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( true , new FloatObj( (float)15 ) , true , new FloatObj( (float)16 ) , true ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( true , new FloatObj( (float)9 ) , true , new FloatObj( (float)9 ) , true ) , false },
      { new FloatRangeBlockIndex( (float)10 , (float)20 ) , new NumberRangeFilter( true , new FloatObj( (float)21 ) , true , new FloatObj( (float)21 ) , true ) , false },

    };
  }

  @Test
  public void T_newInstance_1(){
    FloatRangeBlockIndex bIndex = new FloatRangeBlockIndex( (float)10 , (float)20 );
    assertEquals( (float)10 , bIndex.getMin() );
    assertEquals( (float)20 , bIndex.getMax() );
  }

  @Test
  public void T_newInstance_2(){
    FloatRangeBlockIndex bIndex = new FloatRangeBlockIndex();
    assertEquals( Float.MAX_VALUE , bIndex.getMin() );
    assertEquals( Float.MIN_VALUE , bIndex.getMax() );
  }

  @Test
  public void T_getBlockIndexType_1(){
    IBlockIndex bIndex = new FloatRangeBlockIndex();
    assertEquals( BlockIndexType.RANGE_FLOAT , bIndex.getBlockIndexType() );
  }

  @Test
  public void T_merge_1(){
    FloatRangeBlockIndex bIndex = new FloatRangeBlockIndex( (float)10 , (float)20 );
    assertEquals( (float)10 , bIndex.getMin() );
    assertEquals( (float)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new FloatRangeBlockIndex( (float)110 , (float)150 ) ) );
    assertEquals( (float)10 , bIndex.getMin() );
    assertEquals( (float)150 , bIndex.getMax() );
  }

  @Test
  public void T_merge_2(){
    FloatRangeBlockIndex bIndex = new FloatRangeBlockIndex( (float)10 , (float)20 );
    assertEquals( (float)10 , bIndex.getMin() );
    assertEquals( (float)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new FloatRangeBlockIndex( (float)9 , (float)20 ) ) );
    assertEquals( (float)9 , bIndex.getMin() );
    assertEquals( (float)20 , bIndex.getMax() );
  }

  @Test
  public void T_merge_3(){
    FloatRangeBlockIndex bIndex = new FloatRangeBlockIndex( (float)10 , (float)20 );
    assertEquals( (float)10 , bIndex.getMin() );
    assertEquals( (float)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new FloatRangeBlockIndex( (float)10 , (float)21 ) ) );
    assertEquals( (float)10 , bIndex.getMin() );
    assertEquals( (float)21 , bIndex.getMax() );
  }

  @Test
  public void T_merge_4(){
    FloatRangeBlockIndex bIndex = new FloatRangeBlockIndex( (float)10 , (float)20 );
    assertEquals( (float)10 , bIndex.getMin() );
    assertEquals( (float)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new FloatRangeBlockIndex( (float)9 , (float)21 ) ) );
    assertEquals( (float)9 , bIndex.getMin() );
    assertEquals( (float)21 , bIndex.getMax() );
  }

  @Test
  public void T_merge_5(){
    FloatRangeBlockIndex bIndex = new FloatRangeBlockIndex( (float)10 , (float)20 );
    assertEquals( (float)10 , bIndex.getMin() );
    assertEquals( (float)20 , bIndex.getMax() );
    assertFalse( bIndex.merge( UnsupportedBlockIndex.INSTANCE ) );
  }

  @Test
  public void T_getBinarySize_1(){
    FloatRangeBlockIndex bIndex = new FloatRangeBlockIndex( (float)10 , (float)20 );
    assertEquals( Float.BYTES * 2 , bIndex.getBinarySize() );
  }

  @Test
  public void T_binary_1(){
    FloatRangeBlockIndex bIndex = new FloatRangeBlockIndex( (float)10 , (float)20 );
    byte[] binary = bIndex.toBinary();
    assertEquals( binary.length , bIndex.getBinarySize() );
    FloatRangeBlockIndex bIndex2 = new FloatRangeBlockIndex();
    assertEquals( Float.MAX_VALUE , bIndex2.getMin() );
    assertEquals( Float.MIN_VALUE , bIndex2.getMax() );
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
