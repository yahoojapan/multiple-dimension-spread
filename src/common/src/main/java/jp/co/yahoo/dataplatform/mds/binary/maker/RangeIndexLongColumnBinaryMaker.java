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

import jp.co.yahoo.dataplatform.schema.objects.LongObj;

import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveCell;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.maker.index.RangeLongIndex;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.blockindex.LongRangeBlockIndex;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class RangeIndexLongColumnBinaryMaker extends UniqLongColumnBinaryMaker{

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column , final MakerCache makerCache ) throws IOException{
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    Map<Long,Integer> dicMap = new HashMap<Long,Integer>();
    int columnIndexLength = column.size() * Integer.BYTES;
    int dicBufferSize = ( column.size() + 1 ) * Long.BYTES;
    byte[] binaryRaw = new byte[ ( Integer.BYTES * 2 ) + columnIndexLength + dicBufferSize ];
    ByteBuffer indexWrapBuffer = ByteBuffer.wrap( binaryRaw , 0 , Integer.BYTES + columnIndexLength );
    ByteBuffer dicLengthBuffer = ByteBuffer.wrap( binaryRaw , Integer.BYTES + columnIndexLength , Integer.BYTES );
    ByteBuffer dicWrapBuffer = ByteBuffer.wrap( binaryRaw , Integer.BYTES * 2 + columnIndexLength , dicBufferSize );
    indexWrapBuffer.putInt( columnIndexLength );

    dicMap.put( null , Integer.valueOf(0) );
    dicWrapBuffer.putLong( Long.valueOf( (long)0 ) );

    Long min = Long.MAX_VALUE;
    Long max = Long.MIN_VALUE;
    int rowCount = 0;
    boolean hasNull = false;
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      Long target = null;
      if( cell.getType() != ColumnType.NULL ){
        rowCount++;
        PrimitiveCell stringCell = (PrimitiveCell) cell;
        target = Long.valueOf( stringCell.getRow().getLong() );
      }
      else{
        hasNull = true;
      }
      if( ! dicMap.containsKey( target ) ){
        if( 0 < min.compareTo( target ) ){
          min = Long.valueOf( target );
        }
        if( max.compareTo( target ) < 0 ){
          max = Long.valueOf( target );
        }
        dicMap.put( target , dicMap.size() );
        dicWrapBuffer.putLong( target.longValue() );
      }
      indexWrapBuffer.putInt( dicMap.get( target ) );
    }

    if( ! hasNull && min.equals( max ) ){
      return ConstantColumnBinaryMaker.createColumnBinary( new LongObj( min ) , column.getColumnName() , column.size() );
    }

    int dicLength = dicMap.size() * Long.BYTES;
    int dataLength = binaryRaw.length - ( dicBufferSize - dicLength );
    dicLengthBuffer.putInt( dicLength );
    byte[] compressBinary = currentConfig.compressorClass.compress( binaryRaw , 0 , dataLength );

    byte[] binary = new byte[ Long.BYTES * 2 + compressBinary.length ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    wrapBuffer.putLong( min );
    wrapBuffer.putLong( max );
    wrapBuffer.put( compressBinary );

    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.LONG , rowCount , dataLength , rowCount * Long.BYTES , dicMap.size() , binary , 0 , binary.length , null );
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Long min = Long.valueOf( wrapBuffer.getLong() );
    Long max = Long.valueOf( wrapBuffer.getLong() );
    return new HeaderIndexLazyColumn(
      columnBinary.columnName ,
      columnBinary.columnType ,
      new LongColumnManager(
        columnBinary ,
        columnBinary.binaryStart + ( Long.BYTES * 2 ) ,
        columnBinary.binaryLength - ( Long.BYTES * 2 )
      )
      , new RangeLongIndex( min , max )
    );
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    loadInMemoryStorage( columnBinary , allocator , columnBinary.binaryStart + ( Long.BYTES * 2 ) , columnBinary.binaryLength - ( Long.BYTES * 2 ) );
  }

  @Override
  public void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Long min = Long.valueOf( wrapBuffer.getLong() );
    Long max = Long.valueOf( wrapBuffer.getLong() );
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new LongRangeBlockIndex( min , max ) );
  }

}
