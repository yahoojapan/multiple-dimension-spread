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
import java.nio.ByteBuffer;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import org.testng.annotations.Test;

public class TestBinaryUtil{

  @Test
  public void T_toLengthBytesBinary_1() throws IOException{
    byte[] target = "abcd".getBytes( "UTF-8" );
    byte[] binary = BinaryUtil.toLengthBytesBinary( target );
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary );
    int length = wrapBuffer.getInt();
    assertEquals( target.length , length );
    byte[] decodeBytes = new byte[ target.length ];
    wrapBuffer.get( decodeBytes );
    assertEquals( true , Arrays.equals( target , decodeBytes ) );
  }

  @Test
  public void T_toLengthBytesBinary_2() throws IOException{
    byte[] target = "aあいうえおb".getBytes( "UTF-8" );
    byte[] success = "あいうえお".getBytes( "UTF-8" );
    byte[] binary = BinaryUtil.toLengthBytesBinary( target , 1 , target.length - 2 );
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary );
    int length = wrapBuffer.getInt();
    assertEquals( success.length , length );
    byte[] decodeBytes = new byte[ success.length ];
    wrapBuffer.get( decodeBytes );
    assertEquals( true , Arrays.equals( success , decodeBytes ) );
  }


}
