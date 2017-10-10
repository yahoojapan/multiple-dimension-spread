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

public class TestRangeBlockIndexNameShortCut{

  @DataProvider(name = "T_get_1")
  public Object[][] data1() {
    return new Object[][] {
      { "jp.co.yahoo.dataplatform.mds.blockindex.ByteRangeBlockIndex" , "R0" },
      { "jp.co.yahoo.dataplatform.mds.blockindex.ShortRangeBlockIndex" , "R1" },
      { "jp.co.yahoo.dataplatform.mds.blockindex.IntegerRangeBlockIndex" , "R2" },
      { "jp.co.yahoo.dataplatform.mds.blockindex.LongRangeBlockIndex" , "R3" },
      { "jp.co.yahoo.dataplatform.mds.blockindex.FloatRangeBlockIndex" , "R4" },
      { "jp.co.yahoo.dataplatform.mds.blockindex.DoubleRangeBlockIndex" , "R5" },
      { "jp.co.yahoo.dataplatform.mds.blockindex.StringRangeBlockIndex" , "R6" },
    };
  }

  @Test( dataProvider = "T_get_1" )
  public void T_get_1( final String c , final String sc ) throws IOException{
    assertEquals( sc , RangeBlockIndexNameShortCut.getShortCutName( c ) );
    assertEquals( c , RangeBlockIndexNameShortCut.getClassName( sc ) );
  }

}
