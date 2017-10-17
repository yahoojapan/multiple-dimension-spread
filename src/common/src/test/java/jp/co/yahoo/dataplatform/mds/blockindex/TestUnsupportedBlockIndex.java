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

public class TestUnsupportedBlockIndex{

  @Test
  public void T_getBlockIndexType_1(){
    assertEquals( BlockIndexType.UNSUPPORTED , UnsupportedBlockIndex.INSTANCE.getBlockIndexType() );
  }

  @Test
  public void T_merge_1(){
    assertFalse( UnsupportedBlockIndex.INSTANCE.merge( null ) );
  }

  @Test
  public void T_getBinarySize_1(){
    assertEquals( 0 , UnsupportedBlockIndex.INSTANCE.getBinarySize() );
  }

  @Test
  public void T_binary_1(){
    byte[] binary = UnsupportedBlockIndex.INSTANCE.toBinary();
    assertEquals( 0 , binary.length );
  }

  public void T_canBlockSkip_1(){
    assertFalse( UnsupportedBlockIndex.INSTANCE.canBlockSkip( null ) );
  }

}
