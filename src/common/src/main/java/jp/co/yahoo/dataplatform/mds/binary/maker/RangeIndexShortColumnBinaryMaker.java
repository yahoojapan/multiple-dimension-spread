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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.compressor.FindCompressor;
import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;
import jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength;
import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveCell;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.binary.BinaryUtil;
import jp.co.yahoo.dataplatform.mds.binary.BinaryDump;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.maker.index.RangeShortIndex;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.blockindex.ShortRangeBlockIndex;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class RangeIndexShortColumnBinaryMaker extends UniqShortColumnBinaryMaker {

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column , final MakerCache makerBuffer ) throws IOException{
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    Map<Short,Integer> dicMap = new HashMap<Short,Integer>();
    List<Integer> columnIndexList = new ArrayList<Integer>();
    List<Short> dicList = new ArrayList<Short>();

    dicMap.put( null , Integer.valueOf(0) );
    dicList.add( Short.valueOf( (byte)0 ) );

    int rowCount = 0;
    short min = Short.MAX_VALUE;
    short max = Short.MIN_VALUE;
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      Short target = null;
      if( cell.getType() != ColumnType.NULL ){
        rowCount++;
        PrimitiveCell stringCell = (PrimitiveCell) cell;
        target = Short.valueOf( stringCell.getRow().getShort() );
      }
      if( ! dicMap.containsKey( target ) ){
        if( target < min ){
          min = target;
        }
        if( max < target ){
          max = target;
        }
        dicMap.put( target , dicList.size() );
        dicList.add( target );
      }
      columnIndexList.add( dicMap.get( target ) );
    }

    byte[] columnIndexBinaryRaw = BinaryUtil.toLengthBytesBinary( BinaryDump.dumpInteger( columnIndexList ) );
    byte[] dicRawBinary = BinaryUtil.toLengthBytesBinary( BinaryDump.dumpShort( dicList ) );

    byte[] binaryRaw = new byte[ columnIndexBinaryRaw.length + dicRawBinary.length ];
    int offset = 0;
    System.arraycopy( columnIndexBinaryRaw , 0 , binaryRaw , offset , columnIndexBinaryRaw.length );
    offset += columnIndexBinaryRaw.length;
    System.arraycopy( dicRawBinary , 0 , binaryRaw , offset , dicRawBinary.length );

    byte[] binary = currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length );
    byte[] indexBinary = new byte[ ( PrimitiveByteLength.SHORT_LENGTH * 2 ) + binary.length ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( indexBinary , 0 , indexBinary.length );
    wrapBuffer.putShort( min );
    wrapBuffer.putShort( max );
    wrapBuffer.put( binary );

    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.SHORT , rowCount , binaryRaw.length , rowCount * PrimitiveByteLength.SHORT_LENGTH , dicMap.size() , indexBinary , 0 , indexBinary.length , null );
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    short min = wrapBuffer.getShort();
    short max = wrapBuffer.getShort();
    return new HeaderIndexLazyColumn(
      columnBinary.columnName ,
      columnBinary.columnType ,
      new ShortColumnManager(
        columnBinary ,
        primitiveObjectConnector ,
        columnBinary.binaryStart + ( PrimitiveByteLength.SHORT_LENGTH * 2 ) ,
        columnBinary.binaryLength - ( PrimitiveByteLength.SHORT_LENGTH * 2 )
      )
      , new RangeShortIndex( min , max )
    );
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    loadInMemoryStorage( columnBinary , allocator , columnBinary.binaryStart + ( PrimitiveByteLength.SHORT_LENGTH * 2 ) , columnBinary.binaryLength - ( PrimitiveByteLength.SHORT_LENGTH * 2 ) );
  }

  @Override
  public void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    short min = wrapBuffer.getShort();
    short max = wrapBuffer.getShort();
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new ShortRangeBlockIndex( min , max ) );
  }

}
