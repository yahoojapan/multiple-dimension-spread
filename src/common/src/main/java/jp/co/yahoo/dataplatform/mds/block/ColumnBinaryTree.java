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

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import java.io.IOException;
import java.nio.ByteBuffer;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnTypeFactory;
import jp.co.yahoo.dataplatform.mds.util.ByteArrayData;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

public class ColumnBinaryTree{

  private final List<ColumnBinary> currentColumnBinaryList = new ArrayList<ColumnBinary>();
  private final Map<String,ColumnBinaryTree> childTreeMap = new HashMap<String,ColumnBinaryTree>();

  private ColumnNameNode columnNameNode;
  private BlockReadOffset blockReadOffset;
  private int currentCount;
  private int childCount;
  private int metaLength;
  private int allBinaryStart;
  private int allBinaryLength;

  public ColumnBinaryTree(){
    columnNameNode = new ColumnNameNode( "root" );
    columnNameNode.setNeedAllChild( true );
  }

  public ColumnBinary getColumnBinary( final int index ){
    return currentColumnBinaryList.get( index );
  }

  public List<ColumnBinary> getChildColumnBinary( final int index ){
    List<ColumnBinary> result = new ArrayList<ColumnBinary>();
    for( Map.Entry<String,ColumnBinaryTree> entry : childTreeMap.entrySet() ){
      result.add( entry.getValue().getColumnBinary( index ) );
    }
    return result;
  }

  public void add( final ColumnBinary columnBinary ) throws IOException{
    currentCount++;
    currentColumnBinaryList.add( columnBinary );
    if( columnBinary == null ){
      addChild( null );
    }
    else{
      metaLength += columnBinary.getMetaSize();
      addChild( columnBinary.columnBinaryList );
    }
  }

  public void addChild( final List<ColumnBinary> columnBinaryList ) throws IOException{
    childCount++;
    if( columnBinaryList != null ){
      for( ColumnBinary childColumnBinary : columnBinaryList ){
        ColumnBinaryTree childTree = childTreeMap.get( childColumnBinary.columnName );
        if( childTree == null ){
          childTree = new ColumnBinaryTree();
          while( childTree.size() < ( getChildSize() - 1 ) ){
            childTree.add( null );
          }
          childTreeMap.put( childColumnBinary.columnName , childTree );
        }
        childTree.add( childColumnBinary );
      }
    }

    for( Map.Entry<String,ColumnBinaryTree> entry : childTreeMap.entrySet() ){
      ColumnBinaryTree childTree = entry.getValue();
      while( childTree.size() < getChildSize() ){
        childTree.add( null );
      }
    }
  }

  public int size(){
    return currentCount;
  }

  public int getChildSize(){
    return childCount;
  }

  public List<BlockReadOffset> getBlockReadOffset(){
    List<BlockReadOffset> blockReadOffsetList = new ArrayList<BlockReadOffset>();
    if( blockReadOffset != null ){
      blockReadOffsetList.add( blockReadOffset );
    }
    for( Map.Entry<String,ColumnBinaryTree> entry : childTreeMap.entrySet() ){
      blockReadOffsetList.addAll( entry.getValue().getBlockReadOffset() );
    }

    return blockReadOffsetList;
  }

  public void setColumnFilter( final ColumnNameNode columnNameNode ){
    if( columnNameNode == null ){
      return;
    }
    this.columnNameNode = columnNameNode;
  }

  public int toColumnBinaryTree( final byte[] metaBinary , final int start ) throws IOException{
    return toColumnBinaryTree( metaBinary , start , columnNameNode.isNeedAllChild() );
  }

  public int toColumnBinaryTree( final byte[] metaBinary , final int start , final boolean isNeedAllChild ) throws IOException{
    ByteBuffer byteBuffer = ByteBuffer.wrap( metaBinary , start , ( metaBinary.length - start ) );
    int offset = start;
    int childSize =  byteBuffer.getInt( offset );
    offset += PrimitiveByteLength.INT_LENGTH;
    for( int i = 0 ; i < childSize ; i++ ){
      int childNameLength =  byteBuffer.getInt( offset );
      offset += PrimitiveByteLength.INT_LENGTH;
      String childName = new String( metaBinary , offset , childNameLength );
      offset += childNameLength;
      ColumnBinaryTree childColumnBinary = new ColumnBinaryTree();
      boolean isAppend = true;
      if( isNeedAllChild ){
        isAppend = true;
      }
      else{
        if( columnNameNode.containsChild( childName ) ){
          childColumnBinary.setColumnFilter( columnNameNode.getChild( childName ) );
          isAppend = true;
        }
        // array column childName is "ARRAY"
        else if( ColumnTypeFactory.getColumnTypeFromName( childName ) == ColumnType.ARRAY ){
          childColumnBinary.setColumnFilter( columnNameNode );
          isAppend = true;
        }
        else{
          ColumnNameNode childColumnNameNode = new ColumnNameNode( childName , true );
          childColumnNameNode.setNeedAllChild( false );
          childColumnBinary.setColumnFilter( childColumnNameNode );
          isAppend = false;
        }
      }
      offset = childColumnBinary.toColumnBinaryTree( metaBinary , offset ); 
      if( isAppend ){
        childCount = childColumnBinary.size();
        childTreeMap.put( childName , childColumnBinary );
      }
    }
    allBinaryStart = byteBuffer.getInt( offset );
    offset += PrimitiveByteLength.INT_LENGTH;
    allBinaryLength =  byteBuffer.getInt( offset );
    offset += PrimitiveByteLength.INT_LENGTH;
    int currentMetaBinaryLength =  byteBuffer.getInt( offset );
    offset += PrimitiveByteLength.INT_LENGTH;
    if( currentMetaBinaryLength != 0 ){
      byte[] childBuffer = null;
      int childStartDataOffset = 0;
      if( ! columnNameNode.isDisable() ){
        childBuffer = new byte[allBinaryLength];
      }
      for( int startOffset = offset ; offset < startOffset + currentMetaBinaryLength ; ){
        currentCount++; 
        int index = byteBuffer.getInt( offset );
        offset += PrimitiveByteLength.INT_LENGTH;
        int metaBinaryLength = byteBuffer.getInt( offset );
        offset += PrimitiveByteLength.INT_LENGTH;
        if( columnNameNode.isDisable() || metaBinaryLength == 0 ){
          currentColumnBinaryList.add( null );
        }
        else{
          List<ColumnBinary> childList = new ArrayList<ColumnBinary>();
          for( Map.Entry<String,ColumnBinaryTree> entry : childTreeMap.entrySet() ){
            ColumnBinary childColumnBinary = entry.getValue().getColumnBinary( index );
            if( childColumnBinary != null ){
              childList.add( childColumnBinary );
            }
          }
          ColumnBinary childColumnBinary = ColumnBinary.newInstanceFromMetaBinary( metaBinary , offset , metaBinaryLength , childBuffer , childList );
          childColumnBinary.binaryStart = childStartDataOffset;
          childStartDataOffset += childColumnBinary.binaryLength;
          if( allBinaryLength != 0 ){
            blockReadOffset = new BlockReadOffset( allBinaryStart , allBinaryLength , childBuffer );
          }
          currentColumnBinaryList.add( childColumnBinary );
        }
        offset += metaBinaryLength;
      }
    }

    return offset;
  }

  public void create( final ByteArrayData metaBuffer , final ByteArrayData buffer ) throws IOException{
    byte[] binaryOffsetMetaData = new byte[PrimitiveByteLength.INT_LENGTH * 3];
    if( ! currentColumnBinaryList.isEmpty() ){
      byte[] currentMetaBinary = new byte[ ( PrimitiveByteLength.INT_LENGTH * 2 ) * currentColumnBinaryList.size() + metaLength ];
      ByteBuffer wrapMetaBinaryBuffer = ByteBuffer.wrap( currentMetaBinary );
      int currentMetaBinaryOffset = 0;

      allBinaryStart = buffer.getLength();
      for( int i = 0 ; i < currentColumnBinaryList.size() ; i++ ){
        ColumnBinary columnBinary = currentColumnBinaryList.get(i); 
        wrapMetaBinaryBuffer.putInt( currentMetaBinaryOffset , i );
        currentMetaBinaryOffset+= PrimitiveByteLength.INT_LENGTH;
        if( columnBinary == null ){
          wrapMetaBinaryBuffer.putInt( currentMetaBinaryOffset , 0 );
          currentMetaBinaryOffset+= PrimitiveByteLength.INT_LENGTH;
        }
        else{
          int binaryStart = buffer.getLength();
          buffer.append( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
          int binaryLength = buffer.getLength() - binaryStart;
          columnBinary.binaryStart = binaryStart;
          columnBinary.binaryLength = binaryLength;
          byte[] metaBinary = columnBinary.toMetaBinary();
          byte[] lengthMetaBinary = new byte[ PrimitiveByteLength.INT_LENGTH + metaBinary.length ];
          ByteBuffer metaWrapBuffer = ByteBuffer.wrap( lengthMetaBinary );
          metaWrapBuffer.putInt( metaBinary.length );
          metaWrapBuffer.put( metaBinary );
          System.arraycopy( lengthMetaBinary , 0 , currentMetaBinary , currentMetaBinaryOffset , lengthMetaBinary.length );
          currentMetaBinaryOffset+=lengthMetaBinary.length;
        }
      }
      allBinaryLength = buffer.getLength() - allBinaryStart;
      binaryOffsetMetaData = new byte[ PrimitiveByteLength.INT_LENGTH * 3 + currentMetaBinary.length ];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryOffsetMetaData );
      wrapBuffer.putInt( allBinaryStart );
      wrapBuffer.putInt( allBinaryLength );
      wrapBuffer.putInt( currentMetaBinary.length );
      System.arraycopy( currentMetaBinary , 0 , binaryOffsetMetaData , wrapBuffer.position() , currentMetaBinary.length );
    }
 
    int childSize = childTreeMap.size();
    byte[] childSizeBytes = new byte[PrimitiveByteLength.INT_LENGTH];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( childSizeBytes );
    wrapBuffer.putInt( 0 , childSize );
    metaBuffer.append( childSizeBytes );

    for( Map.Entry<String,ColumnBinaryTree> entry : childTreeMap.entrySet() ){
      byte[] childNameBytes = entry.getKey().getBytes( "UTF-8" );
      byte[] lengthBytes = new byte[ PrimitiveByteLength.INT_LENGTH + childNameBytes.length ];
      ByteBuffer childWrapBuffer = ByteBuffer.wrap( lengthBytes );
      childWrapBuffer.putInt( childNameBytes.length );
      childWrapBuffer.put( childNameBytes );
      metaBuffer.append( lengthBytes );
      entry.getValue().create( metaBuffer , buffer );
    }

    metaBuffer.append( binaryOffsetMetaData );
  }

  public void clear(){
    for( Map.Entry<String,ColumnBinaryTree> entry : childTreeMap.entrySet() ){
      entry.getValue().clear();
    }
    currentColumnBinaryList.clear();
    childTreeMap.clear();
    columnNameNode = null;
    currentCount = 0;
    childCount = 0;
    metaLength = 0;
    childCount = 0;
    allBinaryLength = 0;
  }

}
