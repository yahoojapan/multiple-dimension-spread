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
package jp.co.yahoo.dataplatform.mds.binary.maker.index;

import java.io.IOException;
import java.nio.IntBuffer;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import org.testng.annotations.Test;
import org.testng.annotations.DataProvider;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;

import jp.co.yahoo.dataplatform.mds.binary.maker.IDicManager;
import jp.co.yahoo.dataplatform.mds.spread.column.*;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.*;
import jp.co.yahoo.dataplatform.mds.spread.expression.*;

import jp.co.yahoo.dataplatform.schema.objects.*;

import jp.co.yahoo.dataplatform.mds.spread.column.index.ICellIndex;

public class TestRangeStringIndex{

  private final boolean[] dummy = new boolean[0];

  @DataProvider(name = "T_filter_1")
  public Object[][] data1() {
    return new Object[][] {
      { new RangeStringIndex( "10" , "20" , true ) , new PerfectMatchStringFilter( "21" ) , dummy },
      { new RangeStringIndex( "10" , "20" , true ) , new PerfectMatchStringFilter( "09" ) , dummy },
      { new RangeStringIndex( "10" , "20" , true ) , new PerfectMatchStringFilter( "15" ) , null },
      { new RangeStringIndex( "10" , "20" , true ) , new PerfectMatchStringFilter( "10" ) , null },
      { new RangeStringIndex( "10" , "20" , true ) , new PerfectMatchStringFilter( "20" ) , null },

      { new RangeStringIndex( "10" , "20" , true ) , new LtStringCompareFilter("09" ) , dummy },
      { new RangeStringIndex( "10" , "20" , true ) , new LtStringCompareFilter( "10" ) , dummy },
      { new RangeStringIndex( "10" , "20" , true ) , new LtStringCompareFilter( "11" ) , null },

      { new RangeStringIndex( "10" , "20" , true ) , new LeStringCompareFilter( "09" ) , dummy },
      { new RangeStringIndex( "10" , "20" , true ) , new LeStringCompareFilter( "10" ) , null },
      { new RangeStringIndex( "10" , "20" , true ) , new LeStringCompareFilter( "11" ) , null },

      { new RangeStringIndex( "10" , "20" , true ) , new GtStringCompareFilter( "21" ) , dummy },
      { new RangeStringIndex( "10" , "20" , true ) , new GtStringCompareFilter( "20" ) , dummy },
      { new RangeStringIndex( "10" , "20" , true ) , new GtStringCompareFilter( "19" ) , null },

      { new RangeStringIndex( "10" , "20" , true ) , new GeStringCompareFilter( "21" ) , dummy },
      { new RangeStringIndex( "10" , "20" , true ) , new GeStringCompareFilter( "20" ) , null },
      { new RangeStringIndex( "10" , "20" , true ) , new GeStringCompareFilter( "19" ) , null },

      { new RangeStringIndex( "10" , "20" , true ) , new RangeStringCompareFilter( "10" , true , "10" , true ) , null },
      { new RangeStringIndex( "00" , "20" , true ) , new RangeStringCompareFilter( "05" , true , "15" , true ) , null },
      { new RangeStringIndex( "10" , "20" , true ) , new RangeStringCompareFilter( "20" , true , "21" , true ) , null },
      { new RangeStringIndex( "10" , "20" , true ) , new RangeStringCompareFilter( "15" , true , "25" , true ) , null },
      { new RangeStringIndex( "10" , "20" , true ) , new RangeStringCompareFilter( "10" , true , "20" , true ) , null },
      { new RangeStringIndex( "10" , "20" , true ) , new RangeStringCompareFilter( "10" , true , "10" , true ) , null },
      { new RangeStringIndex( "10" , "20" , true ) , new RangeStringCompareFilter( "20" , true , "20" , true ) , null },
      { new RangeStringIndex( "10" , "20" , true ) , new RangeStringCompareFilter( "15" , true , "16" , true ) , null },
      { new RangeStringIndex( "10" , "20" , true ) , new RangeStringCompareFilter( "09" , true , "09" , true ) , dummy },
      { new RangeStringIndex( "10" , "20" , true ) , new RangeStringCompareFilter( "21" , true , "21" , true ) , dummy },

      { new RangeStringIndex( "10" , "20" , true ) , new RangeStringCompareFilter( "10" , true , "10" , true , true ) , null },
      { new RangeStringIndex( "10" , "20" , true ) , new RangeStringCompareFilter( "05" , true , "15" , true , true ) , null },
      { new RangeStringIndex( "10" , "20" , true ) , new RangeStringCompareFilter( "20" , true , "21" , true , true ) , null },
      { new RangeStringIndex( "10" , "20" , true ) , new RangeStringCompareFilter( "15" , true , "25" , true , true ) , null },
      { new RangeStringIndex( "10" , "20" , true ) , new RangeStringCompareFilter( "10" , true , "20" , true , true ) , dummy },
      { new RangeStringIndex( "10" , "20" , true ) , new RangeStringCompareFilter( "10" , true , "10" , true , true ) , null },
      { new RangeStringIndex( "10" , "20" , true ) , new RangeStringCompareFilter( "20" , true , "20" , true , true ) , null },
      { new RangeStringIndex( "10" , "20" , true ) , new RangeStringCompareFilter( "15" , true , "16" , true , true ) , null },
      { new RangeStringIndex( "10" , "20" , true ) , new RangeStringCompareFilter( "09" , true , "09" , true , true ) , null },
      { new RangeStringIndex( "10" , "20" , true ) , new RangeStringCompareFilter( "21" , true , "21" , true , true ) , null },

      { new RangeStringIndex( "a__0" , "j__9" , true ) , new RangeStringCompareFilter( "b" , true , "b__1" , true , true ) , null },
    };
  }

  @Test( dataProvider = "T_filter_1" )
  public void T_filter_1( final ICellIndex cIndex , final IFilter filter , final boolean[] result ) throws IOException{
    boolean[] r = cIndex.filter( filter , new boolean[0] );
    if( r == null ){
      assertNull( result );
    }
    else{
      assertEquals( result.length , 0 );
    }
  }

}
