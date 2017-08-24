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
package jp.co.yahoo.dataplatform.mds.binary.maker;

import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.Map;
import java.util.HashMap;

import jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength;
import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveCell;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.maker.index.RangeByteIndex;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.blockindex.ByteRangeBlockIndex;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class RangeIndexByteColumnBinaryMaker extends UniqByteColumnBinaryMaker{

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column , final MakerCache makerCache ) throws IOException{
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    Map<Byte,Integer> dicMap = new HashMap<Byte,Integer>();
    int columnIndexLength = column.size() * PrimitiveByteLength.INT_LENGTH;
    int dicBufferSize = ( column.size() + 1 ) * PrimitiveByteLength.BYTE_LENGTH;
    byte[] binaryRaw = new byte[ ( PrimitiveByteLength.INT_LENGTH * 2 ) + columnIndexLength + dicBufferSize ];
    ByteBuffer indexWrapBuffer = ByteBuffer.wrap( binaryRaw , 0 , PrimitiveByteLength.INT_LENGTH + columnIndexLength );
    ByteBuffer dicLengthBuffer = ByteBuffer.wrap( binaryRaw , PrimitiveByteLength.INT_LENGTH + columnIndexLength , PrimitiveByteLength.INT_LENGTH );
    ByteBuffer dicWrapBuffer = ByteBuffer.wrap( binaryRaw , PrimitiveByteLength.INT_LENGTH * 2 + columnIndexLength , dicBufferSize );
    indexWrapBuffer.putInt( columnIndexLength );

    dicMap.put( null , Integer.valueOf(0) );
    dicWrapBuffer.put( Byte.valueOf( (byte)0 ) );

    Byte min = Byte.MAX_VALUE;
    Byte max = Byte.MIN_VALUE;
    int rowCount = 0;
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      Byte target = null;
      if( cell.getType() != ColumnType.NULL ){
        rowCount++;
        PrimitiveCell stringCell = (PrimitiveCell) cell;
        target = Byte.valueOf( stringCell.getRow().getByte() );
      }
      if( ! dicMap.containsKey( target ) ){
        if( 0 < min.compareTo( target ) ){
          min = Byte.valueOf( target );
        }
        if( max.compareTo( target ) < 0 ){
          max = Byte.valueOf( target );
        }
        dicMap.put( target , dicMap.size() );
        dicWrapBuffer.put( target.byteValue() );
      }
      indexWrapBuffer.putInt( dicMap.get( target ) );
    }

    int dicLength = dicMap.size() * PrimitiveByteLength.BYTE_LENGTH;
    int dataLength = binaryRaw.length - ( dicBufferSize - dicLength );
    dicLengthBuffer.putInt( dicLength );
    byte[] compressBinary = currentConfig.compressorClass.compress( binaryRaw , 0 , dataLength );

    byte[] binary = new byte[ PrimitiveByteLength.BYTE_LENGTH * 2 + compressBinary.length ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    wrapBuffer.put( min );
    wrapBuffer.put( max );
    wrapBuffer.put( compressBinary );

    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.BYTE , rowCount , dataLength , rowCount * PrimitiveByteLength.BYTE_LENGTH , dicMap.size() , binary , 0 , binary.length , null );
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Byte min = Byte.valueOf( wrapBuffer.get() );
    Byte max = Byte.valueOf( wrapBuffer.get() );
    return new HeaderIndexLazyColumn(
      columnBinary.columnName ,
      columnBinary.columnType ,
      new ByteColumnManager(
        columnBinary ,
        primitiveObjectConnector ,
        columnBinary.binaryStart + ( PrimitiveByteLength.BYTE_LENGTH * 2 ) ,
        columnBinary.binaryLength - ( PrimitiveByteLength.BYTE_LENGTH * 2 )
      )
      , new RangeByteIndex( min , max )
    );
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    loadInMemoryStorage( columnBinary , allocator , columnBinary.binaryStart + ( PrimitiveByteLength.BYTE_LENGTH * 2 ) , columnBinary.binaryLength - ( PrimitiveByteLength.BYTE_LENGTH * 2 ) );
  }

  @Override
  public void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Byte min = Byte.valueOf( wrapBuffer.get() );
    Byte max = Byte.valueOf( wrapBuffer.get() );
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new ByteRangeBlockIndex( min , max ) );
  }

}
