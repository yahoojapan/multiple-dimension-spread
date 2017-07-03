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
package jp.co.yahoo.dataplatform.mds.binary;

import java.io.IOException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import jp.co.yahoo.dataplatform.mds.binary.maker.*;
import org.testng.annotations.Test;
import org.testng.annotations.DataProvider;

import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

public class TestColumnBinaryMakerConfig{

  @DataProvider(name = "D_T_getColumnMaker_1")
  public Object[][] D_T_addRowNotException(){
    return new Object[][]{
      { ColumnType.UNION , DumpUnionColumnBinaryMaker.class.getName() },
      { ColumnType.ARRAY , DumpArrayColumnBinaryMaker.class.getName() },
      { ColumnType.SPREAD , DumpSpreadColumnBinaryMaker.class.getName() },
      { ColumnType.BOOLEAN , DumpBooleanColumnBinaryMaker.class.getName() },
      { ColumnType.BYTE , UniqByteColumnBinaryMaker.class.getName() },
      { ColumnType.BYTES , DumpBytesColumnBinaryMaker.class.getName() },
      { ColumnType.DOUBLE , UniqDoubleColumnBinaryMaker.class.getName() },
      { ColumnType.FLOAT , UniqFloatColumnBinaryMaker.class.getName() },
      { ColumnType.INTEGER , UniqIntegerColumnBinaryMaker.class.getName() },
      { ColumnType.LONG , UniqLongColumnBinaryMaker.class.getName() },
      { ColumnType.SHORT , UniqShortColumnBinaryMaker.class.getName() },
      { ColumnType.STRING , UniqStringToUTF8BytesColumnBinaryMaker.class.getName() },
      { ColumnType.NULL , UnsupportedColumnBinaryMaker.class.getName() },
      { ColumnType.EMPTY_ARRAY , UnsupportedColumnBinaryMaker.class.getName() },
      { ColumnType.EMPTY_SPREAD , UnsupportedColumnBinaryMaker.class.getName() },
      { ColumnType.UNKNOWN , UnsupportedColumnBinaryMaker.class.getName() },
    };
  }

  @Test
  public void T_newInstance_1() throws IOException{
    new ColumnBinaryMakerConfig();
  }

  @Test
  public void T_newInstance_2() throws IOException{
    new ColumnBinaryMakerConfig( new ColumnBinaryMakerConfig() );
  }

  @Test(dataProvider = "D_T_getColumnMaker_1")
  public void T_getColumnMaker_1( final ColumnType columnType , final String className ) throws IOException{
    ColumnBinaryMakerConfig config = new ColumnBinaryMakerConfig();
    IColumnBinaryMaker maker = config.getColumnMaker( columnType );
    assertEquals( maker.getClass().getName() , className );
  }


}
