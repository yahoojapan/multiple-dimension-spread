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
package jp.co.yahoo.dataplatform.mds.binary.blockindex;

import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.Map;
import java.util.HashMap;

import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

public class BlockIndexNode{

  private final Map<String,BlockIndexNode> childContainer;

  private IBlockIndex blockIndex;
  private boolean isDisable;

  public BlockIndexNode(){
    childContainer = new HashMap<String,BlockIndexNode>();
  }

  public void setBlockIndex( final IBlockIndex blockIndex ){
    if( isDisable ){
      return;
    }
    if( this.blockIndex == null ){
      this.blockIndex = blockIndex;
    }
    else{
      if( ! this.blockIndex.merge( blockIndex ) ){
        disable();
      }
    }
  }

  public void disable(){
    childContainer.clear();
    blockIndex = null;
    isDisable = true;
  }

  public IBlockIndex getBlockIndex(){
    if( blockIndex == null ){
      return UnsupportedBlockIndex.INSTANCE;
    }
    return blockIndex;
  }

  public BlockIndexNode getChildNode( final String nodeName ){
    if( ! childContainer.containsKey( nodeName ) ){
      childContainer.put( nodeName , new BlockIndexNode() );
    }
    return childContainer.get( nodeName );
  }

  public void putChildNode( final String nodeName , final BlockIndexNode node ){
    childContainer.put( nodeName , node );
  }

  public int getBinarySize() throws IOException{
    if( isDisable ){
      return 0;
    }
    int length = 0;
    length += 4;
    if( blockIndex != null ){
      length += 4;
      length += blockIndex.getClass().getName().getBytes( "UTF-8" ).length;
      length += 4;
      length += blockIndex.getBinarySize();
    }
    length += 4;
    for( Map.Entry<String,BlockIndexNode> entry : childContainer.entrySet() ){
      int childLength = entry.getValue().getBinarySize();
      if( childLength != 0 ){
        length += 4;
        length += 4;
        length += entry.getKey().getBytes( "UTF-8" ).length;
        length += 4;
        length += childLength;
      }
    }
    return length;
  }

  public int toBinary( final byte[] buffer , final int start ) throws IOException{
    if( isDisable ){
      return start;
    }
    int offset = start;
    ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer );
    if( blockIndex == null ){
      wrapBuffer.putInt( offset , 0 );
      offset += 4;
    }
    else{
      wrapBuffer.putInt( offset , 1 );
      offset += 4;
      byte[] rangeClassNameBytes = blockIndex.getClass().getName().getBytes( "UTF-8" );
      wrapBuffer.putInt( offset , rangeClassNameBytes.length );
      offset += 4;
      wrapBuffer.position( offset );
      wrapBuffer.put( rangeClassNameBytes );
      offset += rangeClassNameBytes.length;
      byte[] indexBinary = blockIndex.toBinary();
      wrapBuffer.putInt( offset , indexBinary.length );
      offset += 4;
      wrapBuffer.position( offset );
      wrapBuffer.put( indexBinary );
      offset += indexBinary.length;
    }
    int childCountOffset = offset;
    int childCount = 0;
    offset += 4;
    for( Map.Entry<String,BlockIndexNode> entry : childContainer.entrySet() ){
      byte[] childKeyNameBytes = entry.getKey().getBytes( "UTF-8" );
      int childBinaryLengthOffset = offset;
      offset += 4;
      int childKeyNameLengthOffset = offset;
      offset += 4;
      int childKeyNameOffset = offset;
      offset += childKeyNameBytes.length;
      int childEndOffset = entry.getValue().toBinary( buffer , offset );
      if( childEndOffset != offset ){
        wrapBuffer.putInt( childBinaryLengthOffset , childEndOffset - offset );
        wrapBuffer.putInt( childKeyNameLengthOffset , childKeyNameBytes.length );
        wrapBuffer.position( childKeyNameOffset );
        wrapBuffer.put( childKeyNameBytes );
        childCount++;
        offset = childEndOffset;
      }
      else{
        offset = childBinaryLengthOffset;
      }
    }
    wrapBuffer.putInt( childCountOffset , childCount );
    return offset;
  }

  public void clear(){
    childContainer.clear();
    blockIndex = null;
    isDisable = false;
  }

  public static BlockIndexNode createFromBinary( final byte[] buffer , final int start ) throws IOException{
    BlockIndexNode result = new BlockIndexNode();
    int offset = start;
    ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer );
    int currentBlockIndexExists = wrapBuffer.getInt( offset );
    offset += 4;
    if( currentBlockIndexExists == 1 ){
      int classNameLength = wrapBuffer.getInt( offset );
      offset += 4;
      byte[] classNameBytes = new byte[ classNameLength ];
      wrapBuffer.position( offset );
      wrapBuffer.get( classNameBytes , 0 , classNameLength );
      offset += classNameLength;
      int indexBinaryLength = wrapBuffer.getInt( offset );
      offset += 4;
      byte[] indexBinary = new byte[indexBinaryLength];
      wrapBuffer.position( offset );
      wrapBuffer.get( indexBinary , 0 , indexBinaryLength );
      offset += indexBinaryLength;
      IBlockIndex blockIndex = FindBlockIndex.get( new String( classNameBytes , "UTF-8" ) );
      blockIndex.setFromBinary( indexBinary , 0 , indexBinary.length );
      result.setBlockIndex( blockIndex );
    }
    int childCount = wrapBuffer.getInt( offset );
    offset += 4;
    for( int i = 0 ; i < childCount ; i++ ){
      int childBinaryLength = wrapBuffer.getInt( offset );
      offset += 4;
      int childNameLength = wrapBuffer.getInt( offset );
      offset += 4;
      byte[] childNameBytes = new byte[childNameLength];
      wrapBuffer.position( offset );
      wrapBuffer.get( childNameBytes , 0 , childNameBytes.length );
      offset += childNameBytes.length;
      BlockIndexNode childNode = createFromBinary( buffer , offset );
      offset += childBinaryLength;
      result.putChildNode( new String( childNameBytes , "UTF-8" ) , childNode );
    }
    return result;
  }

}
