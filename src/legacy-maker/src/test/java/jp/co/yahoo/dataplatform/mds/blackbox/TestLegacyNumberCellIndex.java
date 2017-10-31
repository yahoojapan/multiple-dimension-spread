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

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

public class TestLegacyNumberCellIndex extends TestNumberCellIndex{

  @DataProvider(name = "target_class")
  public Object[][] data1() throws IOException{
    return new Object[][] {
      { createByteTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpByteColumnBinaryMaker" ) },
      { createByteTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.UniqByteColumnBinaryMaker" ) },
      { createByteTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.RangeDumpByteColumnBinaryMaker" ) },
      { createByteTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.RangeIndexByteColumnBinaryMaker" ) },

      { createShortTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpShortColumnBinaryMaker" ) },
      { createShortTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.UniqShortColumnBinaryMaker" ) },
      { createShortTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.RangeDumpShortColumnBinaryMaker" ) },
      { createShortTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.RangeIndexShortColumnBinaryMaker" ) },

      { createIntTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpIntegerColumnBinaryMaker" ) },
      { createIntTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.UniqIntegerColumnBinaryMaker" ) },
      { createIntTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.RangeDumpIntegerColumnBinaryMaker" ) },
      { createIntTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.RangeIndexIntegerColumnBinaryMaker" ) },

      { createLongTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpLongColumnBinaryMaker" ) },
      { createLongTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.UniqLongColumnBinaryMaker" ) },
      { createLongTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.RangeDumpLongColumnBinaryMaker" ) },
      { createLongTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.RangeIndexLongColumnBinaryMaker" ) },

      { createFloatTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.UniqFloatColumnBinaryMaker" ) },
      { createFloatTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.RangeIndexFloatColumnBinaryMaker" ) },

      { createDoubleTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.UniqDoubleColumnBinaryMaker" ) },
      { createDoubleTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.RangeIndexDoubleColumnBinaryMaker" ) },

      { createStringTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpStringColumnBinaryMaker" ) },
      { createStringTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.FullRangeDumpStringColumnBinaryMaker" ) },
      { createStringTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.FullRangeIndexStringToUTF8BytesColumnBinaryMaker" ) },
      { createStringTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.RangeDumpStringColumnBinaryMaker" ) },
      { createStringTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.RangeIndexStringToUTF8BytesColumnBinaryMaker" ) },
      { createStringTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.UniqStringColumnBinaryMaker" ) },
      { createStringTestData( "jp.co.yahoo.dataplatform.mds.binary.maker.UniqStringToUTF8BytesColumnBinaryMaker" ) },
    };
  }

}
