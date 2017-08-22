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

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength;
import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveCell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.binary.BinaryUtil;
import jp.co.yahoo.dataplatform.mds.binary.BinaryDump;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.maker.index.RangeIntegerIndex;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.blockindex.IntegerRangeBlockIndex;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class RangeIndexIntegerColumnBinaryMaker extends UniqIntegerColumnBinaryMaker{

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column , final MakerCache makerBuffer ) throws IOException{
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    Map<Integer,Integer> dicMap = new HashMap<Integer,Integer>();
    List<Integer> columnIndexList = new ArrayList<Integer>();
    List<Integer> dicList = new ArrayList<Integer>();

    dicMap.put( null , Integer.valueOf(0) );
    dicList.add( Integer.valueOf( (byte)0 ) );

    int rowCount = 0;
    int min = Integer.MAX_VALUE;
    int max = Integer.MIN_VALUE;
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      Integer target = null;
      if( cell.getType() != ColumnType.NULL ){
        rowCount++;
        PrimitiveCell stringCell = (PrimitiveCell) cell;
        target = Integer.valueOf( stringCell.getRow().getInt() );
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
    byte[] dicRawBinary = BinaryUtil.toLengthBytesBinary( BinaryDump.dumpInteger( dicList ) );

    byte[] binaryRaw = new byte[ columnIndexBinaryRaw.length + dicRawBinary.length ];
    int offset = 0;
    System.arraycopy( columnIndexBinaryRaw , 0 , binaryRaw , offset , columnIndexBinaryRaw.length );
    offset += columnIndexBinaryRaw.length;
    System.arraycopy( dicRawBinary , 0 , binaryRaw , offset , dicRawBinary.length );

    byte[] binary = currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length );
    byte[] indexBinary = new byte[ ( PrimitiveByteLength.INT_LENGTH * 2 ) + binary.length ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( indexBinary , 0 , indexBinary.length );
    wrapBuffer.putInt( min );
    wrapBuffer.putInt( max );
    wrapBuffer.put( binary );

    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.INTEGER , rowCount , binaryRaw.length , PrimitiveByteLength.INT_LENGTH * rowCount , dicMap.size() , indexBinary , 0 , indexBinary.length , null );
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    int min = wrapBuffer.getInt();
    int max = wrapBuffer.getInt();
    return new HeaderIndexLazyColumn(
      columnBinary.columnName ,
      columnBinary.columnType ,
      new IntegerColumnManager(
        columnBinary ,
        primitiveObjectConnector ,
        columnBinary.binaryStart + ( PrimitiveByteLength.INT_LENGTH * 2 ) ,
        columnBinary.binaryLength - ( PrimitiveByteLength.INT_LENGTH * 2 )
      )
      , new RangeIntegerIndex( min , max )
    );
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    loadInMemoryStorage( columnBinary , allocator , columnBinary.binaryStart + ( PrimitiveByteLength.INT_LENGTH * 2 ) , columnBinary.binaryLength - ( PrimitiveByteLength.INT_LENGTH * 2 ) );
  }

  @Override
  public void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    int min = wrapBuffer.getInt();
    int max = wrapBuffer.getInt();
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new IntegerRangeBlockIndex( min , max ) );
  }

}
