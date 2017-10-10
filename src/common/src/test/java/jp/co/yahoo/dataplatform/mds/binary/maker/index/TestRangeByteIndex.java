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

public class TestRangeByteIndex{

  private final boolean[] dummy = new boolean[0];

  @DataProvider(name = "T_filter_1")
  public Object[][] data1() {
    return new Object[][] {
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ByteObj( (byte)21 ) ) , dummy },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ByteObj( (byte)9 ) ) , dummy },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ByteObj( (byte)15 ) ) , null },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ByteObj( (byte)10 ) ) , null },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ByteObj( (byte)20 ) ) , null },

      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.LT , new ByteObj( (byte)9 ) ) , dummy },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.LT , new ByteObj( (byte)10 ) ) , dummy },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.LT , new ByteObj( (byte)11 ) ) , null },

      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.LE , new ByteObj( (byte)9 ) ) , dummy },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.LE , new ByteObj( (byte)10 ) ) , null },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.LE , new ByteObj( (byte)11 ) ) , null },

      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.GT , new ByteObj( (byte)21 ) ) , dummy },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.GT , new ByteObj( (byte)20 ) ) , dummy },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.GT , new ByteObj( (byte)19 ) ) , null },

      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.GE , new ByteObj( (byte)21 ) ) , dummy },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.GE , new ByteObj( (byte)20 ) ) , null },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberFilter( NumberFilterType.GE , new ByteObj( (byte)19 ) ) , null },

      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)10 ) , true , new ByteObj( (byte)10 ) , true ) , null },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)5 ) , true , new ByteObj( (byte)15 ) , true ) , null },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)20 ) , true , new ByteObj( (byte)21 ) , true ) , null },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)15 ) , true , new ByteObj( (byte)25 ) , true ) , null },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)10 ) , true , new ByteObj( (byte)20 ) , true ) , null },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)10 ) , true , new ByteObj( (byte)10 ) , true ) , null },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)20 ) , true , new ByteObj( (byte)20 ) , true ) , null },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)15 ) , true , new ByteObj( (byte)16 ) , true ) , null },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)9 ) , true , new ByteObj( (byte)9 ) , true ) , dummy },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( false , new ByteObj( (byte)21 ) , true , new ByteObj( (byte)21 ) , true ) , dummy },

      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)10 ) , true , new ByteObj( (byte)10 ) , true ) , dummy },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)5 ) , true , new ByteObj( (byte)15 ) , true ) , dummy },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)20 ) , true , new ByteObj( (byte)21 ) , true ) , dummy },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)15 ) , true , new ByteObj( (byte)25 ) , true ) , dummy },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)10 ) , true , new ByteObj( (byte)20 ) , true ) , dummy },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)10 ) , true , new ByteObj( (byte)10 ) , true ) , dummy },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)20 ) , true , new ByteObj( (byte)20 ) , true ) , dummy },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)15 ) , true , new ByteObj( (byte)16 ) , true ) , dummy },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)9 ) , true , new ByteObj( (byte)9 ) , true ) , null },
      { new RangeByteIndex( (byte)10 , (byte)20 ) , new NumberRangeFilter( true , new ByteObj( (byte)21 ) , true , new ByteObj( (byte)21 ) , true ) , null },

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
