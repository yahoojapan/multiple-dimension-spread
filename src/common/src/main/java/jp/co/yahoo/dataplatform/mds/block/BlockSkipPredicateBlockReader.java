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
import java.io.InputStream;
import java.nio.ByteBuffer;

import java.util.List;
import java.util.Set;
import java.util.HashSet;

import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;

import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionNode;
import jp.co.yahoo.dataplatform.mds.compressor.FindCompressor;
import jp.co.yahoo.dataplatform.mds.compressor.CompressorNameShortCut;
import jp.co.yahoo.dataplatform.mds.util.InputStreamUtils;

public class BlockSkipPredicateBlockReader extends PredicateBlockReader{

  private BlockIndexNode blockIndexNode = new BlockIndexNode();
  private IExpressionNode blockSkipIndex;

  public BlockSkipPredicateBlockReader(){
    super();
  }

  @Override
  public void setBlockSkipIndex( final IExpressionNode blockSkipIndex ){
    this.blockSkipIndex = blockSkipIndex;
  }


  @Override
  public void setStream( final InputStream in , final int blockSize ) throws IOException{
    super.clear();
    byte[] compressorClassLengthBytes = new byte[Integer.BYTES]; 
    InputStreamUtils.read( in , compressorClassLengthBytes , 0 , Integer.BYTES );
    int compressorClassLength = ByteBuffer.wrap( compressorClassLengthBytes ).getInt();
    byte[] compressorClassBytes = new byte[ compressorClassLength ];
    InputStreamUtils.read( in , compressorClassBytes , 0 , compressorClassBytes.length );
    compressor = FindCompressor.get( CompressorNameShortCut.getClassName( new String( compressorClassBytes , "UTF-8" ) ) );

    byte[] blockIndexLengthBytes = new byte[Integer.BYTES]; 
    InputStreamUtils.read( in , blockIndexLengthBytes , 0 , Integer.BYTES );
    int blockIndexLength = ByteBuffer.wrap( blockIndexLengthBytes ).getInt();
    byte[] blockIndexBinary = new byte[ blockIndexLength ];
    InputStreamUtils.read( in , blockIndexBinary , 0 , blockIndexBinary.length );

    blockIndexNode = BlockIndexNode.createFromBinary( blockIndexBinary , 0 );
    expandFunction.expandIndexNode( blockIndexNode );
    flattenFunction.flattenIndexNode( blockIndexNode );

    List<Integer> blockIndexList = null;
    if( blockSkipIndex != null ){
      blockIndexList = blockSkipIndex.getBlockSpreadIndex( blockIndexNode );
    }
    if( blockIndexList != null && blockIndexList.isEmpty() ){
      InputStreamUtils.skip( in , blockSize - ( 4 + compressorClassLength + 4 + blockIndexBinary.length ) );
    }
    else{
      Set<Integer> spreadIndexDict = null;
      if( blockIndexList != null ){
        spreadIndexDict = new HashSet<Integer>( blockIndexList );
      }
      super.setStream( in , blockSize - ( 4 + compressorClassLength + 4 + blockIndexBinary.length ) , spreadIndexDict );
    }
  }

}
