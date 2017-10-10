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

public class TestIntegerRangeBlockIndex{

  @DataProvider(name = "T_canBlockSkip_1")
  public Object[][] data1() {
    return new Object[][] {
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberFilter( NumberFilterType.EQUAL , new IntegerObj( (int)21 ) ) , true },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberFilter( NumberFilterType.EQUAL , new IntegerObj( (int)9 ) ) , true },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberFilter( NumberFilterType.EQUAL , new IntegerObj( (int)15 ) ) , false },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberFilter( NumberFilterType.EQUAL , new IntegerObj( (int)10 ) ) , false },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberFilter( NumberFilterType.EQUAL , new IntegerObj( (int)20 ) ) , false },

      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberFilter( NumberFilterType.LT , new IntegerObj( (int)9 ) ) , true },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberFilter( NumberFilterType.LT , new IntegerObj( (int)10 ) ) , true },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberFilter( NumberFilterType.LT , new IntegerObj( (int)11 ) ) , false },

      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberFilter( NumberFilterType.LE , new IntegerObj( (int)9 ) ) , true },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberFilter( NumberFilterType.LE , new IntegerObj( (int)10 ) ) , false },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberFilter( NumberFilterType.LE , new IntegerObj( (int)11 ) ) , false },

      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberFilter( NumberFilterType.GT , new IntegerObj( (int)21 ) ) , true },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberFilter( NumberFilterType.GT , new IntegerObj( (int)20 ) ) , true },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberFilter( NumberFilterType.GT , new IntegerObj( (int)19 ) ) , false },

      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberFilter( NumberFilterType.GE , new IntegerObj( (int)21 ) ) , true },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberFilter( NumberFilterType.GE , new IntegerObj( (int)20 ) ) , false },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberFilter( NumberFilterType.GE , new IntegerObj( (int)19 ) ) , false },

      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( false , new IntegerObj( (int)10 ) , true , new IntegerObj( (int)10 ) , true ) , false },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( false , new IntegerObj( (int)5 ) , true , new IntegerObj( (int)15 ) , true ) , false },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( false , new IntegerObj( (int)20 ) , true , new IntegerObj( (int)21 ) , true ) , false },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( false , new IntegerObj( (int)15 ) , true , new IntegerObj( (int)25 ) , true ) , false },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( false , new IntegerObj( (int)10 ) , true , new IntegerObj( (int)20 ) , true ) , false },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( false , new IntegerObj( (int)10 ) , true , new IntegerObj( (int)10 ) , true ) , false },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( false , new IntegerObj( (int)20 ) , true , new IntegerObj( (int)20 ) , true ) , false },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( false , new IntegerObj( (int)15 ) , true , new IntegerObj( (int)16 ) , true ) , false },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( false , new IntegerObj( (int)9 ) , true , new IntegerObj( (int)9 ) , true ) , true },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( false , new IntegerObj( (int)21 ) , true , new IntegerObj( (int)21 ) , true ) , true },

      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( true , new IntegerObj( (int)10 ) , true , new IntegerObj( (int)10 ) , true ) , true },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( true , new IntegerObj( (int)5 ) , true , new IntegerObj( (int)15 ) , true ) , true },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( true , new IntegerObj( (int)20 ) , true , new IntegerObj( (int)21 ) , true ) , true },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( true , new IntegerObj( (int)15 ) , true , new IntegerObj( (int)25 ) , true ) , true },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( true , new IntegerObj( (int)10 ) , true , new IntegerObj( (int)20 ) , true ) , true },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( true , new IntegerObj( (int)10 ) , true , new IntegerObj( (int)10 ) , true ) , true },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( true , new IntegerObj( (int)20 ) , true , new IntegerObj( (int)20 ) , true ) , true },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( true , new IntegerObj( (int)15 ) , true , new IntegerObj( (int)16 ) , true ) , true },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( true , new IntegerObj( (int)9 ) , true , new IntegerObj( (int)9 ) , true ) , false },
      { new IntegerRangeBlockIndex( (int)10 , (int)20 ) , new NumberRangeFilter( true , new IntegerObj( (int)21 ) , true , new IntegerObj( (int)21 ) , true ) , false },

    };
  }

  @Test
  public void T_newInstance_1(){
    IntegerRangeBlockIndex bIndex = new IntegerRangeBlockIndex( (int)10 , (int)20 );
    assertEquals( (int)10 , bIndex.getMin() );
    assertEquals( (int)20 , bIndex.getMax() );
  }

  @Test
  public void T_newInstance_2(){
    IntegerRangeBlockIndex bIndex = new IntegerRangeBlockIndex();
    assertEquals( Integer.MAX_VALUE , bIndex.getMin() );
    assertEquals( Integer.MIN_VALUE , bIndex.getMax() );
  }

  @Test
  public void T_getBlockIndexType_1(){
    IBlockIndex bIndex = new IntegerRangeBlockIndex();
    assertEquals( BlockIndexType.RANGE_INTEGER , bIndex.getBlockIndexType() );
  }

  @Test
  public void T_merge_1(){
    IntegerRangeBlockIndex bIndex = new IntegerRangeBlockIndex( (int)10 , (int)20 );
    assertEquals( (int)10 , bIndex.getMin() );
    assertEquals( (int)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new IntegerRangeBlockIndex( (int)110 , (int)150 ) ) );
    assertEquals( (int)10 , bIndex.getMin() );
    assertEquals( (int)150 , bIndex.getMax() );
  }

  @Test
  public void T_merge_2(){
    IntegerRangeBlockIndex bIndex = new IntegerRangeBlockIndex( (int)10 , (int)20 );
    assertEquals( (int)10 , bIndex.getMin() );
    assertEquals( (int)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new IntegerRangeBlockIndex( (int)9 , (int)20 ) ) );
    assertEquals( (int)9 , bIndex.getMin() );
    assertEquals( (int)20 , bIndex.getMax() );
  }

  @Test
  public void T_merge_3(){
    IntegerRangeBlockIndex bIndex = new IntegerRangeBlockIndex( (int)10 , (int)20 );
    assertEquals( (int)10 , bIndex.getMin() );
    assertEquals( (int)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new IntegerRangeBlockIndex( (int)10 , (int)21 ) ) );
    assertEquals( (int)10 , bIndex.getMin() );
    assertEquals( (int)21 , bIndex.getMax() );
  }

  @Test
  public void T_merge_4(){
    IntegerRangeBlockIndex bIndex = new IntegerRangeBlockIndex( (int)10 , (int)20 );
    assertEquals( (int)10 , bIndex.getMin() );
    assertEquals( (int)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new IntegerRangeBlockIndex( (int)9 , (int)21 ) ) );
    assertEquals( (int)9 , bIndex.getMin() );
    assertEquals( (int)21 , bIndex.getMax() );
  }

  @Test
  public void T_merge_5(){
    IntegerRangeBlockIndex bIndex = new IntegerRangeBlockIndex( (int)10 , (int)20 );
    assertEquals( (int)10 , bIndex.getMin() );
    assertEquals( (int)20 , bIndex.getMax() );
    assertFalse( bIndex.merge( UnsupportedBlockIndex.INSTANCE ) );
  }

  @Test
  public void T_getBinarySize_1(){
    IntegerRangeBlockIndex bIndex = new IntegerRangeBlockIndex( (int)10 , (int)20 );
    assertEquals( Integer.BYTES * 2 , bIndex.getBinarySize() );
  }

  @Test
  public void T_binary_1(){
    IntegerRangeBlockIndex bIndex = new IntegerRangeBlockIndex( (int)10 , (int)20 );
    byte[] binary = bIndex.toBinary();
    assertEquals( binary.length , bIndex.getBinarySize() );
    IntegerRangeBlockIndex bIndex2 = new IntegerRangeBlockIndex();
    assertEquals( Integer.MAX_VALUE , bIndex2.getMin() );
    assertEquals( Integer.MIN_VALUE , bIndex2.getMax() );
    bIndex2.setFromBinary( binary , 0 , binary.length );
    assertEquals( bIndex2.getMin() , bIndex.getMin() );
    assertEquals( bIndex2.getMax() , bIndex.getMax() );
  }

  @Test( dataProvider = "T_canBlockSkip_1" )
  public void T_canBlockSkip_1( final IBlockIndex bIndex , final IFilter filter , final boolean result ){
    assertEquals( result , bIndex.canBlockSkip( filter ) );
  }

}
