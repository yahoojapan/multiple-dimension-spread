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
package jp.co.yahoo.dataplatform.mds.spread.analyzer;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.DataProvider;

import jp.co.yahoo.dataplatform.schema.objects.*;

import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.spread.column.NullColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

public class TestShortColumnAnalizeResult {

  @Test
  public void T_getAnalizer_1() throws IOException{
    ShortColumnAnalizeResult result = new ShortColumnAnalizeResult( "name" , 100 , true , 10 , 90 , 2 , (short)40 , (short)50 );
    assertEquals( "name" , result.getColumnName() );
    assertEquals( ColumnType.SHORT , result.getColumnType() );
    assertEquals( 100 , result.getColumnSize() );
    assertEquals( true , result.maybeSorted() );
    assertEquals( 10 , result.getNullCount() );
    assertEquals( 90 , result.getRowCount() );
    assertEquals( 2 , result.getUniqCount() );
    assertEquals( Short.BYTES * 90 , result.getLogicalDataSize() );
    assertEquals( (short)40 , result.getMin() );
    assertEquals( (short)50 , result.getMax() );
  }

}
