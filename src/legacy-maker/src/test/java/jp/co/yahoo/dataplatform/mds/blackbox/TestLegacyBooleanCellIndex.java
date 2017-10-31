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

import java.util.Set;
import java.util.HashSet;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

import jp.co.yahoo.dataplatform.config.Configuration;

import jp.co.yahoo.dataplatform.schema.objects.*;

import jp.co.yahoo.dataplatform.mds.spread.expression.*;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.*;
import jp.co.yahoo.dataplatform.mds.spread.column.*;
import jp.co.yahoo.dataplatform.mds.binary.*;
import jp.co.yahoo.dataplatform.mds.binary.maker.*;

public class TestLegacyBooleanCellIndex extends TestBooleanCellIndex{

  @DataProvider(name = "target_class")
  public Object[][] data1() throws IOException{
    return new Object[][] {
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
