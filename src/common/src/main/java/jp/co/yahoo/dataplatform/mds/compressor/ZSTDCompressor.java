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
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;

import jp.co.yahoo.dataplatform.mds.util.InputStreamUtils;

public class ZSTDCompressor implements ICompressor{

  private static final byte[] LENGTH_DUMMY = new byte[Integer.BYTES];

  @Override
  public byte[] compress( final byte[] data , final int start , final int length ) throws IOException{
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    bOut.write( LENGTH_DUMMY , 0 , LENGTH_DUMMY.length );
    ZstdOutputStream out = new ZstdOutputStream( bOut );

    out.write( data , start , length );
    out.flush();
    out.close();
    byte[] compressByte = bOut.toByteArray();
    ByteBuffer.wrap( compressByte ).putInt( length );

    bOut.close();

    return compressByte;
  }

  @Override                                                                                                   public void compress( final byte[] data , final int start , final int length , final OutputStream out ) throws IOException{
    ZstdOutputStream zstdOut = new ZstdOutputStream( out );

    zstdOut.write( data , start , length );
    zstdOut.flush();
    zstdOut.close();
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

    ByteArrayInputStream bIn = new ByteArrayInputStream( data , start + Integer.BYTES , length );
    ZstdInputStream in = new ZstdInputStream( bIn );

    byte[] retVal = new byte[dataLength];
    InputStreamUtils.read( in , retVal , 0 , dataLength );

    return retVal;
  }

  @Override
  public int decompressAndSet( final byte[] data , final int start , final int length , final byte[] buffer ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( data , start , length );
    int dataLength = wrapBuffer.getInt();

    ByteArrayInputStream bIn = new ByteArrayInputStream( data , start + Integer.BYTES , length );
    ZstdInputStream in = new ZstdInputStream( bIn );

    InputStreamUtils.read( in , buffer , 0 , dataLength );

    return dataLength;
  }

  @Override
  public InputStream getDecompressInputStream( final byte[] data , final int start , final int length ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( data , start , length );
    int dataLength = wrapBuffer.getInt();
    ByteArrayInputStream bIn = new ByteArrayInputStream( data , start + Integer.BYTES , length );
    return new BufferedInputStream( new ZstdInputStream( bIn ) );
  }

}
