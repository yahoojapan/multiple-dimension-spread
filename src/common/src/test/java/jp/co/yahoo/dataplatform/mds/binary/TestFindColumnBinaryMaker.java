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

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import jp.co.yahoo.dataplatform.mds.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.binary.maker.UnsupportedColumnBinaryMaker;

public class TestFindColumnBinaryMaker{

  @Test
  public void T_get_1() throws IOException{
    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( UnsupportedColumnBinaryMaker.class.getName() );
    assertTrue( maker instanceof UnsupportedColumnBinaryMaker );
  }

  @Test( expectedExceptions = { IOException.class } )
  public void T_get_2() throws IOException{
    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( null );
    assertTrue( maker instanceof UnsupportedColumnBinaryMaker );
  }

  @Test( expectedExceptions = { IOException.class } )
  public void T_get_3() throws IOException{
    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( "" );
    assertTrue( maker instanceof UnsupportedColumnBinaryMaker );
  }

  @Test( expectedExceptions = { IOException.class } )
  public void T_get_4() throws IOException{
    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( "java.lang.String" );
  }

  @Test( expectedExceptions = { IOException.class } )
  public void T_get_5() throws IOException{
    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( "____TEST____" );
    assertTrue( maker instanceof UnsupportedColumnBinaryMaker );
  }

}
