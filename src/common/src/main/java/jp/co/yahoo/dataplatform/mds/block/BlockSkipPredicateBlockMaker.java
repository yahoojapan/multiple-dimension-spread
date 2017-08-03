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
package jp.co.yahoo.dataplatform.mds.block;

import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.List;
import java.util.ArrayList;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.FindColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.config.Configuration;

import jp.co.yahoo.dataplatform.schema.parser.IParser;
import jp.co.yahoo.dataplatform.schema.parser.JacksonMessageReader;

import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;
import jp.co.yahoo.dataplatform.mds.compressor.DefaultCompressor;
import jp.co.yahoo.dataplatform.mds.compressor.CompressorNameShortCut;
import jp.co.yahoo.dataplatform.mds.compressor.FindCompressor;
import jp.co.yahoo.dataplatform.mds.util.ByteArrayData;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.maker.MakerCache;

import static jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength.INT_LENGTH;

public class BlockSkipPredicateBlockMaker extends PredicateBlockMaker{

  private BlockIndexNode blockIndexNode = new BlockIndexNode();
  private byte[] compressorClassNameBytes;

  public BlockSkipPredicateBlockMaker() throws IOException{
    super();
    compressor = new DefaultCompressor();
    compressorClassNameBytes = CompressorNameShortCut.getShortCutName( compressor.getClass().getName() ).getBytes( "UTF-8" );
  }

  @Override
  public void setup( final int blockSize , final Configuration config ) throws IOException{
    super.setup( blockSize , config );
    compressor = FindCompressor.get( config.get( "block.maker.compress.class" , "jp.co.yahoo.dataplatform.mds.compressor.DefaultCompressor" ) );
  }

  @Override
  public void appendHeader( final byte[] headerBytes ){
    if( this.headerBytes.length != 0 ){
      byte[] mergeByte = new byte[ this.headerBytes.length + headerBytes.length ];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( mergeByte );
      wrapBuffer.put( this.headerBytes );
      wrapBuffer.put( headerBytes );
      this.headerBytes = mergeByte;
    }
    else{
      this.headerBytes = headerBytes;
    }
  }

  @Override
  public void append( final int spreadSize , final List<ColumnBinary> binaryList ) throws IOException{
    for( ColumnBinary columnBinary : binaryList ){
      if( columnBinary != null ){
        IColumnBinaryMaker maker = FindColumnBinaryMaker.get( columnBinary.makerClassName );
        maker.setBlockIndexNode( blockIndexNode , columnBinary );
      }
    }
    super.append( spreadSize , binaryList );
  }

  @Override
  public int size(){
    try{
      return super.size() + 4 + compressorClassNameBytes.length + blockIndexNode.getBinarySize() + 4;
    }catch( IOException e ){
      throw new RuntimeException( e );
    }
  }

  @Override
  public byte[] create( final int dataSize ) throws IOException{
    byte[] blockIndexBinary = new byte[4 + ( compressor.getClass().getName().length() * 2 ) + blockIndexNode.getBinarySize() + 4];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( blockIndexBinary );
    wrapBuffer.putInt( compressorClassNameBytes.length );
    wrapBuffer.put( compressorClassNameBytes );
    int metaLength = 4 + compressorClassNameBytes.length + 4;
    wrapBuffer.putInt( blockIndexBinary.length - metaLength );
    blockIndexNode.toBinary( blockIndexBinary , metaLength );
    appendHeader( blockIndexBinary );
    blockIndexNode.clear();
    return super.create( dataSize );
  }

  @Override
  public String getReaderClassName(){
    return BlockSkipPredicateBlockReader.class.getName();
  }

  @Override
  public void close() throws IOException{
    super.close();
  }

}
