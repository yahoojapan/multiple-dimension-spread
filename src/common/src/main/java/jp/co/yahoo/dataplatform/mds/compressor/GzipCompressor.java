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

import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;

import jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength;
import jp.co.yahoo.dataplatform.mds.util.InputStreamUtils;

public class GzipCompressor implements ICompressor{

  @Override
  public byte[] compress( final byte[] data , final int start , final int length ) throws IOException{
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    GZIPOutputStream out = new GZIPOutputStream( bOut );

    out.write( data , start , length );
    out.flush();
    out.finish();
    byte[] compressByte = bOut.toByteArray();
    byte[] retVal = new byte[ PrimitiveByteLength.INT_LENGTH + compressByte.length ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( retVal );
    wrapBuffer.putInt( length );
    wrapBuffer.put( compressByte );

    bOut.close();
    out.close();

    return retVal;
  }

  @Override
  public void compress( final byte[] data , final int start , final int length , final OutputStream out ) throws IOException{
    GZIPOutputStream gzipOut = new GZIPOutputStream( out );

    gzipOut.write( data , start , length );
    gzipOut.flush();
    gzipOut.finish();
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
    GZIPInputStream in = new GZIPInputStream( bIn , 1024 * 256 );

    byte[] retVal = new byte[dataLength];
    InputStreamUtils.read( in , retVal , 0 , dataLength );

    return retVal;
  }

  @Override
  public int decompressAndSet( final byte[] data , final int start , final int length , final byte[] buffer ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( data , start , length );
    int dataLength = wrapBuffer.getInt();

    ByteArrayInputStream bIn = new ByteArrayInputStream( data , start + PrimitiveByteLength.INT_LENGTH , length );
    GZIPInputStream in = new GZIPInputStream( bIn , 1024 * 256 );

    InputStreamUtils.read( in , buffer , 0 , dataLength );

    return dataLength;
  }

  @Override
  public InputStream getDecompressInputStream( final byte[] data , final int start , final int length ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( data , start , length );
    int dataLength = wrapBuffer.getInt();
    ByteArrayInputStream bIn = new ByteArrayInputStream( data , start + PrimitiveByteLength.INT_LENGTH , length );
    return new BufferedInputStream( new GZIPInputStream( bIn , 1024 * 256 ) , 1024 * 256 );
  }

}
