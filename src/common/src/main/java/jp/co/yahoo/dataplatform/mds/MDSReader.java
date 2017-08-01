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
package jp.co.yahoo.dataplatform.mds;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength;
import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionNode;
import jp.co.yahoo.dataplatform.config.FindClass;
import jp.co.yahoo.dataplatform.config.Configuration;

import jp.co.yahoo.dataplatform.mds.block.IBlockReader;
import jp.co.yahoo.dataplatform.mds.util.InputStreamUtils;
import jp.co.yahoo.dataplatform.mds.stats.SummaryStats;

public class MDSReader implements AutoCloseable{

  private static final byte[] MAGIC = new byte[]{'$','C','L','M'};

  private final Map<String,IBlockReader> blockReaderMap = new HashMap<String,IBlockReader>();
  private final List<ReadBlockOffset> readTargetList = new ArrayList<ReadBlockOffset>();
  private final SummaryStats readStats = new SummaryStats();
  private IBlockReader currentBlockReader;
  private IExpressionNode blockSkipIndex;

  private InputStream in;
  private int blockSize;
  private long inReadOffset;

  private class FileHeaderMeta {
    public final int blockSize;
    public final int headerSize;
    public final String className;

    public FileHeaderMeta( final int blockSize , final String className , final int headerSize ){
      this.blockSize = blockSize;
      this.className = className;
      this.headerSize = headerSize;
    }
  }

  private class ReadBlockOffset{
    public final long start;
    public final int length;

    public ReadBlockOffset( final long start , final int length ){
      this.start = start;
      this.length = length;
    }
  }

  private FileHeaderMeta readFileHeader( final InputStream in ) throws IOException{
    byte[] magic = new byte[MAGIC.length];
    InputStreamUtils.read( in , magic , 0 , MAGIC.length );

    if( ! Arrays.equals( magic , MAGIC) ){
      throw new IOException( "Invalid binary." );
    }

    byte[] blockSize = new byte[PrimitiveByteLength.INT_LENGTH];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( blockSize );
    InputStreamUtils.read( in , blockSize , 0 , PrimitiveByteLength.INT_LENGTH );

    byte[] blockClassLength = new byte[PrimitiveByteLength.INT_LENGTH];
    ByteBuffer wrapLengthBuffer = ByteBuffer.wrap( blockClassLength );
    InputStreamUtils.read( in , blockClassLength , 0 , PrimitiveByteLength.INT_LENGTH );
    int classNameSize = wrapLengthBuffer.getInt( 0 );

    byte[] blockClass = new byte[classNameSize];
    InputStreamUtils.read( in , blockClass , 0 , classNameSize );
    ByteBuffer classNameBuffer = ByteBuffer.wrap( blockClass );
    CharBuffer viewCharBuffer = classNameBuffer.asCharBuffer();
    char[] classNameChars = new char[ classNameSize / PrimitiveByteLength.CHAR_LENGTH ];
    viewCharBuffer.get( classNameChars );
    String blockReaderClass = new String( classNameChars );

    return new FileHeaderMeta( wrapBuffer.getInt( 0 ) , blockReaderClass , ( MAGIC.length + PrimitiveByteLength.INT_LENGTH + PrimitiveByteLength.INT_LENGTH + classNameSize ) );
  }

  public void setBlockSkipIndex( final IExpressionNode blockSkipIndex ){
    this.blockSkipIndex = blockSkipIndex;
  }

  public void setNewStream( final InputStream in , final long dataSize , final Configuration config ) throws IOException{
    setNewStream( in , dataSize , config , 0 , dataSize );
  }

  public void setNewStream( final InputStream in , final long dataSize , final Configuration config , final long start , final long length ) throws IOException{
    inReadOffset = 0;
    readTargetList.clear();

    this.in = in;

    FileHeaderMeta meta = readFileHeader( in );
    inReadOffset += meta.headerSize;
    if( ! blockReaderMap.containsKey( meta.className ) ){
      IBlockReader blockReader = (IBlockReader)( FindClass.getObject( meta.className ) );
      blockReaderMap.put( meta.className , blockReader );
    }

    currentBlockReader = blockReaderMap.get( meta.className );
    currentBlockReader.setup( config );
    currentBlockReader.setBlockSkipIndex( blockSkipIndex );

    blockSize = meta.blockSize;

    int blockCount = Double.valueOf( Math.ceil( (double)dataSize / (double)blockSize ) ).intValue();
    for( int i = 0 ; i < blockCount ; i++ ){
      int targetBlockSize = blockSize;
      if( i == 0 ){
        targetBlockSize -= meta.headerSize;
      }
      long readStartOffset = (long)i * (long)blockSize;
      if( start <= readStartOffset && readStartOffset < ( start + length ) ){
        readTargetList.add( new ReadBlockOffset( readStartOffset , targetBlockSize ) );
      }
    }
    if( readTargetList.isEmpty() ){
      return;
    }
    ReadBlockOffset readOffset = readTargetList.remove(0);
    inReadOffset += InputStreamUtils.skip( in , readOffset.start - inReadOffset );

    currentBlockReader.setBlockSize( blockSize );
    currentBlockReader.setStream( in , readOffset.length );
    inReadOffset += readOffset.length;
  }

  public boolean hasNext() throws IOException{
    if( currentBlockReader.hasNext() ){
      return true;
    }
    else if( ! readTargetList.isEmpty() ){
      return true;
    }
    return false;
  }


  public Spread next() throws IOException{
    if( ! currentBlockReader.hasNext() ){
      if( readTargetList.isEmpty() ){
        return new Spread();
      }
      ReadBlockOffset readOffset = readTargetList.remove(0);
      inReadOffset += InputStreamUtils.skip( in , readOffset.start - inReadOffset );
      currentBlockReader.setStream( in , readOffset.length );
      inReadOffset += readOffset.length;
      if( ! currentBlockReader.hasNext() ){
        return next();
      }
    }
    return currentBlockReader.next();
  }

  public List<ColumnBinary> nextRaw() throws IOException{
    if( ! currentBlockReader.hasNext() ){
      if( readTargetList.isEmpty() ){
        return new ArrayList<ColumnBinary>();
      }
      ReadBlockOffset readOffset = readTargetList.remove(0);
      inReadOffset += InputStreamUtils.skip( in , readOffset.start - inReadOffset );
      currentBlockReader.setStream( in , readOffset.length );
      inReadOffset += readOffset.length;
      if( ! currentBlockReader.hasNext() ){
        return nextRaw();
      }
    }
    return currentBlockReader.nextRaw();
  }

  public int getBlockReadCount(){
    return currentBlockReader.getBlockReadCount();
  }

  public int getBlockCount(){
    return currentBlockReader.getBlockCount();
  }

  public long getReadPos(){
    return inReadOffset;
  }

  public Integer getCurrentSpreadSize(){
    return currentBlockReader.getCurrentSpreadSize();
  }

  public SummaryStats getReadStats(){
    return currentBlockReader.getReadStats();
  }

  public void close() throws IOException{
    if( in != null ){
      in.close();
      in = null;
    }
    inReadOffset = 0;
    readTargetList.clear();
    currentBlockReader.close();
  }

}
