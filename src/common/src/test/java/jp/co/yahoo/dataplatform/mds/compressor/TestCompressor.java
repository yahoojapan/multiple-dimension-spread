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
package jp.co.yahoo.dataplatform.mds.compressor;

import java.io.IOException;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;
import org.testng.annotations.Test;
import org.testng.annotations.DataProvider;

public class TestCompressor {

  private String[] getCompressorClass(){
    return new String[]{
      DefaultCompressor.class.getName(),
      GzipCompressor.class.getName(),
      LZ4Compressor.class.getName()
    };
  }

  @DataProvider(name = "T_compress_1")
  public Object[][] data() throws IOException{
    return new Object[][] {
      { getCompressorClass() , "abcde".getBytes() , 0 , 5 , "abcde".getBytes() },
      { getCompressorClass() , "abcde".getBytes() , 0 , 1 , "a".getBytes() },
      { getCompressorClass() , "abcde".getBytes() , 4 , 1 , "e".getBytes() },
      { getCompressorClass() , "abcde".getBytes() , 1 , 3 , "bcd".getBytes() },
      { getCompressorClass() , "abcde".getBytes() , 0 , 0 , new byte[0] },
    };
  }

  @Test( dataProvider = "T_compress_1")
  public void T_compress_1( final String[] classNames , final byte[] compressTarget , final int start , final int length , final byte[] success ) throws IOException{
    for( int i = 0 ; i < classNames.length ; i++ ){
      ICompressor compressor = FindCompressor.get( classNames[i] );
      byte[] compressData = compressor.compress( compressTarget , start , length );
      assertEquals( compressor.getDecompressSize( compressData , 0 , compressData.length ) , success.length );
      byte[] decompressData = compressor.decompress( compressData , 0 , compressData.length );
      assertTrue( Arrays.equals( decompressData , success ) );
    }
  }

  @Test( dataProvider = "T_compress_1")
  public void T_compressAndSet_1( final String[] classNames , final byte[] compressTarget , final int start , final int length , final byte[] success ) throws IOException{
    for( int i = 0 ; i < classNames.length ; i++ ){
      ICompressor compressor = FindCompressor.get( classNames[i] );
      byte[] compressData = compressor.compress( compressTarget , start , length );
      byte[] decompressData = new byte[ compressor.getDecompressSize( compressData , 0 , compressData.length ) ];
      compressor.decompressAndSet( compressData , 0 , compressData.length , decompressData );
      assertTrue( Arrays.equals( decompressData , success ) );
    }
  }

}
