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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import java.util.List;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.block.FindBlockMaker;
import jp.co.yahoo.dataplatform.mds.block.IBlockMaker;
import jp.co.yahoo.dataplatform.mds.block.BlockSkipPredicateBlockMaker;
import jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength;
import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.config.Configuration;

public class MDSWriter implements AutoCloseable{

  private static final byte[] MAGIC = new byte[]{'$','C','L','M'};

  private final OutputStream out;
  private final IBlockMaker blockMaker;

  public MDSWriter( final OutputStream out , final Configuration config ) throws IOException{
    this.out = out;

    int blockSize = config.getInt( "block.size" , 1024 * 1024 * 128 );

    blockMaker = FindBlockMaker.get( config.get( "block.maker.class" , BlockSkipPredicateBlockMaker.class.getName() ) );
    blockMaker.setup( blockSize , config );
    String blockMakerClassName = blockMaker.getReaderClassName();
    int classNameLength = blockMakerClassName.length() * PrimitiveByteLength.CHAR_LENGTH;

    byte[] header = new byte[MAGIC.length + PrimitiveByteLength.INT_LENGTH + PrimitiveByteLength.INT_LENGTH + classNameLength ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( header );
    CharBuffer viewCharBuffer = wrapBuffer.asCharBuffer();
    int offset = 0;
    wrapBuffer.put( MAGIC , 0 , MAGIC.length );
    offset += MAGIC.length;
    wrapBuffer.putInt( offset , blockSize );
    offset += PrimitiveByteLength.INT_LENGTH;
    wrapBuffer.putInt( offset , classNameLength );
    offset += PrimitiveByteLength.INT_LENGTH;
    viewCharBuffer.position( offset / PrimitiveByteLength.CHAR_LENGTH );
    viewCharBuffer.put( blockMakerClassName.toCharArray() );

    blockMaker.appendHeader( header );
  }

  public void append( final Spread spread ) throws IOException{
    List<ColumnBinary> binaryList = blockMaker.convertRow( spread );
    if( ! blockMaker.canAppend( binaryList ) ){
      byte[] block = blockMaker.createFixedBlock();
      out.write( block , 0 , block.length );
    }
    blockMaker.append( spread.size() , binaryList );
  }

  public void close() throws IOException{
    byte[] block = blockMaker.createVariableBlock();
    out.write( block , 0 , block.length );
    blockMaker.close();
    out.close();
  }


}
