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
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;

import jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength;
import jp.co.yahoo.dataplatform.mds.util.InputStreamUtils;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4BlockInputStream;

public class LZ4Compressor implements ICompressor{

  private static final byte[] LENGTH_DUMMY = new byte[PrimitiveByteLength.INT_LENGTH];

  @Override
  public byte[] compress( final byte[] data , final int start , final int length ) throws IOException{
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    bOut.write( LENGTH_DUMMY , 0 , LENGTH_DUMMY.length );
    LZ4BlockOutputStream out = new LZ4BlockOutputStream( bOut );

    out.write( data , start , length );
    out.flush();
    out.finish();
    byte[] compressByte = bOut.toByteArray();
    ByteBuffer.wrap( compressByte ).putInt( length );

    bOut.close();
    out.close();

    return compressByte;
  }

  @Override
  public int getDecompressSize( final byte[] data , final int start , final int length ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap( data , start , length );
    return wrapBuffer.getInt();
  }

  @Override
  public byte[] decompress( final byte[] data , final int start , final int length ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( data , start , length );
    int dataLength = wrapBuffer.getInt();

    ByteArrayInputStream bIn = new ByteArrayInputStream( data , start + PrimitiveByteLength.INT_LENGTH , length );
    LZ4BlockInputStream in = new LZ4BlockInputStream( bIn );

    byte[] retVal = new byte[dataLength];
    InputStreamUtils.read( in , retVal , 0 , dataLength );

    return retVal;
  }

  @Override
  public int decompressAndSet( final byte[] data , final int start , final int length , final byte[] buffer ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( data , start , length );
    int dataLength = wrapBuffer.getInt();

    ByteArrayInputStream bIn = new ByteArrayInputStream( data , start + PrimitiveByteLength.INT_LENGTH , length );
    LZ4BlockInputStream in = new LZ4BlockInputStream( bIn );

    InputStreamUtils.read( in , buffer , 0 , dataLength );

    return dataLength;
  }

  @Override
  public InputStream getDecompressInputStream( final byte[] data , final int start , final int length ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( data , start , length );
    int dataLength = wrapBuffer.getInt();
    ByteArrayInputStream bIn = new ByteArrayInputStream( data , start + PrimitiveByteLength.INT_LENGTH , length );
    return new LZ4BlockInputStream( bIn );
  }

}
