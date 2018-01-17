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

import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import java.io.IOException;
import java.nio.ByteBuffer;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnTypeFactory;
import jp.co.yahoo.dataplatform.mds.util.ByteArrayData;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

public class ColumnBinaryTree{

  private final List<ColumnBinary> currentColumnBinaryList = new ArrayList<ColumnBinary>();
  private final Map<String,ColumnBinaryTree> childTreeMap = new HashMap<String,ColumnBinaryTree>();
  private final List<BlockReadOffset> blockReadOffsetList = new ArrayList<BlockReadOffset>();

  private ColumnNameNode columnNameNode;
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
      ColumnBinary childColumnBinary = entry.getValue().getColumnBinary( index );
      if( childColumnBinary != null ){
        result.add( childColumnBinary );
      }
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
    List<BlockReadOffset> result = new ArrayList<BlockReadOffset>();
    result.addAll( blockReadOffsetList );
    for( Map.Entry<String,ColumnBinaryTree> entry : childTreeMap.entrySet() ){
      result.addAll( entry.getValue().getBlockReadOffset() );
    }

    return result;
  }

  public void setColumnFilter( final ColumnNameNode columnNameNode ){
    if( columnNameNode == null ){
      return;
    }
    this.columnNameNode = columnNameNode;
  }

  public int toColumnBinaryTree( final byte[] metaBinary , final int start , final Set<Integer> spreadIndexDict ) throws IOException{
    return toColumnBinaryTree( metaBinary , start , columnNameNode.isNeedAllChild() , spreadIndexDict );
  }

  public int toColumnBinaryTree( final byte[] metaBinary , final int start , final boolean isNeedAllChild , final Set<Integer> spreadIndexDict ) throws IOException{
    ByteBuffer byteBuffer = ByteBuffer.wrap( metaBinary , start , ( metaBinary.length - start ) );
    int offset = start;
    int childSize =  byteBuffer.getInt( offset );
    offset += Integer.BYTES;
    for( int i = 0 ; i < childSize ; i++ ){
      int childNameLength =  byteBuffer.getInt( offset );
      offset += Integer.BYTES;
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
      offset = childColumnBinary.toColumnBinaryTree( metaBinary , offset , spreadIndexDict ); 
      if( childCount < childColumnBinary.size() ){
        childCount = childColumnBinary.size();
      }
      if( isAppend ){
        childTreeMap.put( childName , childColumnBinary );
      }
    }
    allBinaryStart = byteBuffer.getInt( offset );
    offset += Integer.BYTES;
    allBinaryLength =  byteBuffer.getInt( offset );
    offset += Integer.BYTES;
    int currentMetaBinaryLength =  byteBuffer.getInt( offset );
    offset += Integer.BYTES;
    if( currentMetaBinaryLength != 0 ){
      byte[] childBuffer = null;
      int childStartDataOffset = 0;
      if( ! columnNameNode.isDisable() ){
        childBuffer = new byte[allBinaryLength];
      }
      for( int startOffset = offset ; offset < startOffset + currentMetaBinaryLength ; ){
        int index = byteBuffer.getInt( offset );
        offset += Integer.BYTES;
        int metaBinaryLength = byteBuffer.getInt( offset );
        offset += Integer.BYTES;
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
          if( spreadIndexDict == null || spreadIndexDict.contains( Integer.valueOf( currentCount ) ) ){
            if( allBinaryLength != 0 ){
              blockReadOffsetList.add( new BlockReadOffset( childColumnBinary.binaryStart , childStartDataOffset , childColumnBinary.binaryLength , childBuffer ) );
            }
            currentColumnBinaryList.add( childColumnBinary );
          }
          else{
            currentColumnBinaryList.add( null );
          }
          childColumnBinary.binaryStart = childStartDataOffset;
          childStartDataOffset += childColumnBinary.binaryLength;
        }
        offset += metaBinaryLength;
        currentCount++; 
      }
      if( allBinaryLength != 0 && currentCount == blockReadOffsetList.size() ){
        blockReadOffsetList.clear();
        blockReadOffsetList.add( new BlockReadOffset( allBinaryStart , 0 , allBinaryLength , childBuffer ) );
      }
    }

    return offset;
  }

  public void create( final ByteArrayData metaBuffer , final ByteArrayData buffer ) throws IOException{
    byte[] binaryOffsetMetaData = new byte[Integer.BYTES * 3];
    if( ! currentColumnBinaryList.isEmpty() ){
      byte[] currentMetaBinary = new byte[ ( Integer.BYTES * 2 ) * currentColumnBinaryList.size() + metaLength ];
      ByteBuffer wrapMetaBinaryBuffer = ByteBuffer.wrap( currentMetaBinary );
      int currentMetaBinaryOffset = 0;

      allBinaryStart = buffer.getLength();
      for( int i = 0 ; i < currentColumnBinaryList.size() ; i++ ){
        ColumnBinary columnBinary = currentColumnBinaryList.get(i); 
        wrapMetaBinaryBuffer.putInt( currentMetaBinaryOffset , i );
        currentMetaBinaryOffset += Integer.BYTES;
        if( columnBinary == null ){
          wrapMetaBinaryBuffer.putInt( currentMetaBinaryOffset , 0 );
          currentMetaBinaryOffset += Integer.BYTES;
        }
        else{
          int binaryStart = buffer.getLength();
          buffer.append( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
          int binaryLength = buffer.getLength() - binaryStart;
          columnBinary.binaryStart = binaryStart;
          columnBinary.binaryLength = binaryLength;
          byte[] metaBinary = columnBinary.toMetaBinary();
          byte[] lengthMetaBinary = new byte[ Integer.BYTES + metaBinary.length ];
          ByteBuffer metaWrapBuffer = ByteBuffer.wrap( lengthMetaBinary );
          metaWrapBuffer.putInt( metaBinary.length );
          metaWrapBuffer.put( metaBinary );
          System.arraycopy( lengthMetaBinary , 0 , currentMetaBinary , currentMetaBinaryOffset , lengthMetaBinary.length );
          currentMetaBinaryOffset+=lengthMetaBinary.length;
        }
      }
      allBinaryLength = buffer.getLength() - allBinaryStart;
      binaryOffsetMetaData = new byte[ Integer.BYTES * 3 + currentMetaBinary.length ];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryOffsetMetaData );
      wrapBuffer.putInt( allBinaryStart );
      wrapBuffer.putInt( allBinaryLength );
      wrapBuffer.putInt( currentMetaBinary.length );
      System.arraycopy( currentMetaBinary , 0 , binaryOffsetMetaData , wrapBuffer.position() , currentMetaBinary.length );
    }
 
    int childSize = childTreeMap.size();
    byte[] childSizeBytes = new byte[Integer.BYTES];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( childSizeBytes );
    wrapBuffer.putInt( 0 , childSize );
    metaBuffer.append( childSizeBytes );

    for( Map.Entry<String,ColumnBinaryTree> entry : childTreeMap.entrySet() ){
      byte[] childNameBytes = entry.getKey().getBytes( "UTF-8" );
      byte[] lengthBytes = new byte[ Integer.BYTES + childNameBytes.length ];
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
    blockReadOffsetList.clear();
    currentCount = 0;
    childCount = 0;
    metaLength = 0;
    childCount = 0;
    allBinaryLength = 0;
  }

}
