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

import jp.co.yahoo.dataplatform.mds.spread.column.filter.*;

public class TestStringRangeBlockIndex{

  @DataProvider(name = "T_canBlockSkip_1")
  public Object[][] data1() {
    return new Object[][] {
      { new StringRangeBlockIndex( "10" , "20" ) , new PerfectMatchStringFilter( "21" ) , true },
      { new StringRangeBlockIndex( "10" , "20" ) , new PerfectMatchStringFilter( "09" ) , true },
      { new StringRangeBlockIndex( "10" , "20" ) , new PerfectMatchStringFilter( "15" ) , false },
      { new StringRangeBlockIndex( "10" , "20" ) , new PerfectMatchStringFilter( "10" ) , false },
      { new StringRangeBlockIndex( "10" , "20" ) , new PerfectMatchStringFilter( "20" ) , false },

      { new StringRangeBlockIndex( "10" , "20" ) , new LtStringCompareFilter("09" ) , true },
      { new StringRangeBlockIndex( "10" , "20" ) , new LtStringCompareFilter( "10" ) , true },
      { new StringRangeBlockIndex( "10" , "20" ) , new LtStringCompareFilter( "11" ) , false },

      { new StringRangeBlockIndex( "10" , "20" ) , new LeStringCompareFilter( "09" ) , true },
      { new StringRangeBlockIndex( "10" , "20" ) , new LeStringCompareFilter( "10" ) , false },
      { new StringRangeBlockIndex( "10" , "20" ) , new LeStringCompareFilter( "11" ) , false },

      { new StringRangeBlockIndex( "10" , "20" ) , new GtStringCompareFilter( "21" ) , true },
      { new StringRangeBlockIndex( "10" , "20" ) , new GtStringCompareFilter( "20" ) , true },
      { new StringRangeBlockIndex( "10" , "20" ) , new GtStringCompareFilter( "19" ) , false },

      { new StringRangeBlockIndex( "10" , "20" ) , new GeStringCompareFilter( "21" ) , true },
      { new StringRangeBlockIndex( "10" , "20" ) , new GeStringCompareFilter( "20" ) , false },
      { new StringRangeBlockIndex( "10" , "20" ) , new GeStringCompareFilter( "19" ) , false },

      { new StringRangeBlockIndex( "10" , "20" ) , new RangeStringCompareFilter( "10" , true , "10" , true ) , false },
      { new StringRangeBlockIndex( "00" , "20" ) , new RangeStringCompareFilter( "05" , true , "15" , true ) , false },
      { new StringRangeBlockIndex( "10" , "20" ) , new RangeStringCompareFilter( "20" , true , "21" , true ) , false },
      { new StringRangeBlockIndex( "10" , "20" ) , new RangeStringCompareFilter( "15" , true , "25" , true ) , false },
      { new StringRangeBlockIndex( "10" , "20" ) , new RangeStringCompareFilter( "10" , true , "20" , true ) , false },
      { new StringRangeBlockIndex( "10" , "20" ) , new RangeStringCompareFilter( "10" , true , "10" , true ) , false },
      { new StringRangeBlockIndex( "10" , "20" ) , new RangeStringCompareFilter( "20" , true , "20" , true ) , false },
      { new StringRangeBlockIndex( "10" , "20" ) , new RangeStringCompareFilter( "15" , true , "16" , true ) , false },
      { new StringRangeBlockIndex( "10" , "20" ) , new RangeStringCompareFilter( "09" , true , "09" , true ) , true },
      { new StringRangeBlockIndex( "10" , "20" ) , new RangeStringCompareFilter( "21" , true , "21" , true ) , true },

      { new StringRangeBlockIndex( "10" , "20" ) , new RangeStringCompareFilter( "10" , true , "10" , true , true ) , false },
      { new StringRangeBlockIndex( "10" , "20" ) , new RangeStringCompareFilter( "05" , true , "15" , true , true ) , false },
      { new StringRangeBlockIndex( "10" , "20" ) , new RangeStringCompareFilter( "20" , true , "21" , true , true ) , false },
      { new StringRangeBlockIndex( "10" , "20" ) , new RangeStringCompareFilter( "15" , true , "25" , true , true ) , false },
      { new StringRangeBlockIndex( "10" , "20" ) , new RangeStringCompareFilter( "10" , true , "20" , true , true ) , true },
      { new StringRangeBlockIndex( "10" , "20" ) , new RangeStringCompareFilter( "10" , true , "10" , true , true ) , false },
      { new StringRangeBlockIndex( "10" , "20" ) , new RangeStringCompareFilter( "20" , true , "20" , true , true ) , false },
      { new StringRangeBlockIndex( "10" , "20" ) , new RangeStringCompareFilter( "15" , true , "16" , true , true ) , false },
      { new StringRangeBlockIndex( "10" , "20" ) , new RangeStringCompareFilter( "09" , true , "09" , true , true ) , false },
      { new StringRangeBlockIndex( "10" , "20" ) , new RangeStringCompareFilter( "21" , true , "21" , true , true ) , false },

    };
  }

  @Test
  public void T_newInstance_1(){
    StringRangeBlockIndex bIndex = new StringRangeBlockIndex( "10" , "20" );
    assertEquals( "10" , bIndex.getMin() );
    assertEquals( "20" , bIndex.getMax() );
  }

  @Test
  public void T_newInstance_2(){
    StringRangeBlockIndex bIndex = new StringRangeBlockIndex();
    assertEquals( null , bIndex.getMin() );
    assertEquals( null , bIndex.getMax() );
  }

  @Test
  public void T_getBlockIndexType_1(){
    IBlockIndex bIndex = new StringRangeBlockIndex();
    assertEquals( BlockIndexType.RANGE_STRING , bIndex.getBlockIndexType() );
  }

  @Test
  public void T_merge_1(){
    StringRangeBlockIndex bIndex = new StringRangeBlockIndex( "10" , "20" );
    assertEquals( "10" , bIndex.getMin() );
    assertEquals( "20" , bIndex.getMax() );
    assertTrue( bIndex.merge( new StringRangeBlockIndex( "110" , "350" ) ) );
    assertEquals( "10" , bIndex.getMin() );
    assertEquals( "350" , bIndex.getMax() );
  }

  @Test
  public void T_merge_2(){
    StringRangeBlockIndex bIndex = new StringRangeBlockIndex( "10" , "20" );
    assertEquals( "10" , bIndex.getMin() );
    assertEquals( "20" , bIndex.getMax() );
    assertTrue( bIndex.merge( new StringRangeBlockIndex( "09" , "20" ) ) );
    assertEquals( "09" , bIndex.getMin() );
    assertEquals( "20" , bIndex.getMax() );
  }

  @Test
  public void T_merge_3(){
    StringRangeBlockIndex bIndex = new StringRangeBlockIndex( "10" , "20" );
    assertEquals( "10" , bIndex.getMin() );
    assertEquals( "20" , bIndex.getMax() );
    assertTrue( bIndex.merge( new StringRangeBlockIndex( "10" , "21" ) ) );
    assertEquals( "10" , bIndex.getMin() );
    assertEquals( "21" , bIndex.getMax() );
  }

  @Test
  public void T_merge_4(){
    StringRangeBlockIndex bIndex = new StringRangeBlockIndex( "10" , "20" );
    assertEquals( "10" , bIndex.getMin() );
    assertEquals( "20" , bIndex.getMax() );
    assertTrue( bIndex.merge( new StringRangeBlockIndex( "09" , "21" ) ) );
    assertEquals( "09" , bIndex.getMin() );
    assertEquals( "21" , bIndex.getMax() );
  }

  @Test
  public void T_merge_5(){
    StringRangeBlockIndex bIndex = new StringRangeBlockIndex( "10" , "20" );
    assertEquals( "10" , bIndex.getMin() );
    assertEquals( "20" , bIndex.getMax() );
    assertFalse( bIndex.merge( UnsupportedBlockIndex.INSTANCE ) );
  }

  @Test
  public void T_getBinarySize_1(){
    StringRangeBlockIndex bIndex = new StringRangeBlockIndex( "10" , "20" );
    assertEquals( Integer.BYTES * 2 + Character.BYTES * 4 , bIndex.getBinarySize() );
  }

  @Test
  public void T_binary_1(){
    StringRangeBlockIndex bIndex = new StringRangeBlockIndex( "10" , "20" );
    byte[] binary = bIndex.toBinary();
    assertEquals( binary.length , bIndex.getBinarySize() );
    StringRangeBlockIndex bIndex2 = new StringRangeBlockIndex();
    assertEquals( null , bIndex2.getMin() );
    assertEquals( null , bIndex2.getMax() );
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
