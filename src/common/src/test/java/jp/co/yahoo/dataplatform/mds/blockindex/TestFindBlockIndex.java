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

public class TestFindBlockIndex{

  @DataProvider(name = "T_get_1")
  public Object[][] data1() {
    return new Object[][] {
      { "jp.co.yahoo.dataplatform.mds.blockindex.ByteRangeBlockIndex" , ByteRangeBlockIndex.class },
      { "jp.co.yahoo.dataplatform.mds.blockindex.ShortRangeBlockIndex" , ShortRangeBlockIndex.class },
      { "jp.co.yahoo.dataplatform.mds.blockindex.IntegerRangeBlockIndex" , IntegerRangeBlockIndex.class },
      { "jp.co.yahoo.dataplatform.mds.blockindex.LongRangeBlockIndex" , LongRangeBlockIndex.class },
      { "jp.co.yahoo.dataplatform.mds.blockindex.FloatRangeBlockIndex" , FloatRangeBlockIndex.class },
      { "jp.co.yahoo.dataplatform.mds.blockindex.DoubleRangeBlockIndex" , DoubleRangeBlockIndex.class },
      { "jp.co.yahoo.dataplatform.mds.blockindex.StringRangeBlockIndex" , StringRangeBlockIndex.class },
    };
  }

  @Test( dataProvider = "T_get_1" )
  public void T_get_1( final String sc , final Class c ) throws IOException{
    IBlockIndex obj = FindBlockIndex.get( sc );
    assertEquals( obj.getClass().getName() ,  c.getName() );
  }

  @Test( expectedExceptions = { IOException.class } )
  public void T_get_2() throws IOException{
    FindBlockIndex.get( null );
  }

  @Test( expectedExceptions = { IOException.class } )
  public void T_get_3() throws IOException{
    FindBlockIndex.get( "" );
  }

  @Test( expectedExceptions = { IOException.class } )
  public void T_get_4() throws IOException{
    FindBlockIndex.get( "java.lang.String" );
  }

  @Test( expectedExceptions = { IOException.class } )
  public void T_get_5() throws IOException{
    FindBlockIndex.get( "___HOGEHOGE__" );
  }

}
