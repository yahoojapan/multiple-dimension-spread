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

public class TestDoubleRangeBlockIndex{

  @DataProvider(name = "T_canBlockSkip_1")
  public Object[][] data1() {
    return new Object[][] {
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberFilter( NumberFilterType.EQUAL , new DoubleObj( (double)21 ) ) , true },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberFilter( NumberFilterType.EQUAL , new DoubleObj( (double)9 ) ) , true },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberFilter( NumberFilterType.EQUAL , new DoubleObj( (double)15 ) ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberFilter( NumberFilterType.EQUAL , new DoubleObj( (double)10 ) ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberFilter( NumberFilterType.EQUAL , new DoubleObj( (double)20 ) ) , false },

      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberFilter( NumberFilterType.LT , new DoubleObj( (double)9 ) ) , true },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberFilter( NumberFilterType.LT , new DoubleObj( (double)10 ) ) , true },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberFilter( NumberFilterType.LT , new DoubleObj( (double)11 ) ) , false },

      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberFilter( NumberFilterType.LE , new DoubleObj( (double)9 ) ) , true },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberFilter( NumberFilterType.LE , new DoubleObj( (double)10 ) ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberFilter( NumberFilterType.LE , new DoubleObj( (double)11 ) ) , false },

      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberFilter( NumberFilterType.GT , new DoubleObj( (double)21 ) ) , true },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberFilter( NumberFilterType.GT , new DoubleObj( (double)20 ) ) , true },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberFilter( NumberFilterType.GT , new DoubleObj( (double)19 ) ) , false },

      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberFilter( NumberFilterType.GE , new DoubleObj( (double)21 ) ) , true },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberFilter( NumberFilterType.GE , new DoubleObj( (double)20 ) ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberFilter( NumberFilterType.GE , new DoubleObj( (double)19 ) ) , false },

      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( false , new DoubleObj( (double)10 ) , true , new DoubleObj( (double)10 ) , true ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( false , new DoubleObj( (double)5 ) , true , new DoubleObj( (double)15 ) , true ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( false , new DoubleObj( (double)20 ) , true , new DoubleObj( (double)21 ) , true ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( false , new DoubleObj( (double)15 ) , true , new DoubleObj( (double)25 ) , true ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( false , new DoubleObj( (double)10 ) , true , new DoubleObj( (double)20 ) , true ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( false , new DoubleObj( (double)10 ) , true , new DoubleObj( (double)10 ) , true ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( false , new DoubleObj( (double)20 ) , true , new DoubleObj( (double)20 ) , true ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( false , new DoubleObj( (double)15 ) , true , new DoubleObj( (double)16 ) , true ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( false , new DoubleObj( (double)9 ) , true , new DoubleObj( (double)9 ) , true ) , true },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( false , new DoubleObj( (double)21 ) , true , new DoubleObj( (double)21 ) , true ) , true },

      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( true , new DoubleObj( (double)10 ) , true , new DoubleObj( (double)10 ) , true ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( true , new DoubleObj( (double)5 ) , true , new DoubleObj( (double)15 ) , true ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( true , new DoubleObj( (double)20 ) , true , new DoubleObj( (double)21 ) , true ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( true , new DoubleObj( (double)15 ) , true , new DoubleObj( (double)25 ) , true ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( true , new DoubleObj( (double)10 ) , true , new DoubleObj( (double)20 ) , true ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( true , new DoubleObj( (double)10 ) , true , new DoubleObj( (double)10 ) , true ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( true , new DoubleObj( (double)20 ) , true , new DoubleObj( (double)20 ) , true ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( true , new DoubleObj( (double)15 ) , true , new DoubleObj( (double)16 ) , true ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( true , new DoubleObj( (double)9 ) , true , new DoubleObj( (double)9 ) , true ) , false },
      { new DoubleRangeBlockIndex( (double)10 , (double)20 ) , new NumberRangeFilter( true , new DoubleObj( (double)21 ) , true , new DoubleObj( (double)21 ) , true ) , false },

    };
  }

  @Test
  public void T_newInstance_1(){
    DoubleRangeBlockIndex bIndex = new DoubleRangeBlockIndex( (double)10 , (double)20 );
    assertEquals( (double)10 , bIndex.getMin() );
    assertEquals( (double)20 , bIndex.getMax() );
  }

  @Test
  public void T_newInstance_2(){
    DoubleRangeBlockIndex bIndex = new DoubleRangeBlockIndex();
    assertEquals( Double.MAX_VALUE , bIndex.getMin() );
    assertEquals( Double.MIN_VALUE , bIndex.getMax() );
  }

  @Test
  public void T_getBlockIndexType_1(){
    IBlockIndex bIndex = new DoubleRangeBlockIndex();
    assertEquals( BlockIndexType.RANGE_DOUBLE , bIndex.getBlockIndexType() );
  }

  @Test
  public void T_merge_1(){
    DoubleRangeBlockIndex bIndex = new DoubleRangeBlockIndex( (double)10 , (double)20 );
    assertEquals( (double)10 , bIndex.getMin() );
    assertEquals( (double)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new DoubleRangeBlockIndex( (double)110 , (double)150 ) ) );
    assertEquals( (double)10 , bIndex.getMin() );
    assertEquals( (double)150 , bIndex.getMax() );
  }

  @Test
  public void T_merge_2(){
    DoubleRangeBlockIndex bIndex = new DoubleRangeBlockIndex( (double)10 , (double)20 );
    assertEquals( (double)10 , bIndex.getMin() );
    assertEquals( (double)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new DoubleRangeBlockIndex( (double)9 , (double)20 ) ) );
    assertEquals( (double)9 , bIndex.getMin() );
    assertEquals( (double)20 , bIndex.getMax() );
  }

  @Test
  public void T_merge_3(){
    DoubleRangeBlockIndex bIndex = new DoubleRangeBlockIndex( (double)10 , (double)20 );
    assertEquals( (double)10 , bIndex.getMin() );
    assertEquals( (double)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new DoubleRangeBlockIndex( (double)10 , (double)21 ) ) );
    assertEquals( (double)10 , bIndex.getMin() );
    assertEquals( (double)21 , bIndex.getMax() );
  }

  @Test
  public void T_merge_4(){
    DoubleRangeBlockIndex bIndex = new DoubleRangeBlockIndex( (double)10 , (double)20 );
    assertEquals( (double)10 , bIndex.getMin() );
    assertEquals( (double)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new DoubleRangeBlockIndex( (double)9 , (double)21 ) ) );
    assertEquals( (double)9 , bIndex.getMin() );
    assertEquals( (double)21 , bIndex.getMax() );
  }

  @Test
  public void T_merge_5(){
    DoubleRangeBlockIndex bIndex = new DoubleRangeBlockIndex( (double)10 , (double)20 );
    assertEquals( (double)10 , bIndex.getMin() );
    assertEquals( (double)20 , bIndex.getMax() );
    assertFalse( bIndex.merge( UnsupportedBlockIndex.INSTANCE ) );
  }

  @Test
  public void T_getBinarySize_1(){
    DoubleRangeBlockIndex bIndex = new DoubleRangeBlockIndex( (double)10 , (double)20 );
    assertEquals( Double.BYTES * 2 , bIndex.getBinarySize() );
  }

  @Test
  public void T_binary_1(){
    DoubleRangeBlockIndex bIndex = new DoubleRangeBlockIndex( (double)10 , (double)20 );
    byte[] binary = bIndex.toBinary();
    assertEquals( binary.length , bIndex.getBinarySize() );
    DoubleRangeBlockIndex bIndex2 = new DoubleRangeBlockIndex();
    assertEquals( Double.MAX_VALUE , bIndex2.getMin() );
    assertEquals( Double.MIN_VALUE , bIndex2.getMax() );
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
