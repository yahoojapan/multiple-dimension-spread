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

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveCell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;
import jp.co.yahoo.dataplatform.mds.compressor.FindCompressor;
import jp.co.yahoo.dataplatform.mds.binary.BinaryUtil;
import jp.co.yahoo.dataplatform.mds.binary.BinaryDump;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;

import static jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength.INT_LENGTH;
import static jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength.LONG_LENGTH;

public class UniqLongColumnBinaryMaker implements IColumnBinaryMaker{

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column , final MakerCache makerBuffer ) throws IOException{
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    Map<Long,Integer> dicMap = new HashMap<Long,Integer>();
    List<Integer> columnIndexList = new ArrayList<Integer>();
    List<Long> dicList = new ArrayList<Long>();

    dicMap.put( null , Integer.valueOf(0) );
    dicList.add( Long.valueOf( (byte)0 ) );

    int rowCount = 0;
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      Long target = null;
      if( cell.getType() != ColumnType.NULL ){
        rowCount++;
        PrimitiveCell stringCell = (PrimitiveCell) cell;
        target = Long.valueOf( stringCell.getRow().getLong() );
      }
      if( ! dicMap.containsKey( target ) ){
        dicMap.put( target , dicList.size() );
        dicList.add( target );
      }
      columnIndexList.add( dicMap.get( target ) );
    }

    byte[] columnIndexBinaryRaw = BinaryUtil.toLengthBytesBinary( BinaryDump.dumpInteger( columnIndexList ) );
    byte[] dicRawBinary = BinaryUtil.toLengthBytesBinary( BinaryDump.dumpLong( dicList ) );

    byte[] binaryRaw = new byte[ columnIndexBinaryRaw.length + dicRawBinary.length ];
    int offset = 0;
    System.arraycopy( columnIndexBinaryRaw , 0 , binaryRaw , offset , columnIndexBinaryRaw.length );
    offset += columnIndexBinaryRaw.length;
    System.arraycopy( dicRawBinary , 0 , binaryRaw , offset , dicRawBinary.length );

    byte[] binary = currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length );

    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.LONG , rowCount , binaryRaw.length , rowCount * LONG_LENGTH , dicMap.size() , binary , 0 , binary.length , null );
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
    return new LazyColumn( columnBinary.columnName , columnBinary.columnType , new LongColumnManager( columnBinary , primitiveObjectConnector ) );
  }

  public class LongDicManager implements IDicManager{

    private final PrimitiveObject[] dicArray;
    private final int dicSize;

    public LongDicManager( final PrimitiveObject[] dicArray ) throws IOException{
      this.dicArray = dicArray;
      dicSize = dicArray.length;
    }

    @Override
    public PrimitiveObject get( final int index ) throws IOException{
      return dicArray[index];
    }

    @Override
    public int getDicSize() throws IOException{
      return dicSize;
    }
  }

  public class LongColumnManager implements IColumnManager{

    private final IPrimitiveObjectConnector primitiveObjectConnector;
    private final ColumnBinary columnBinary;
    private PrimitiveColumn column;
    private boolean isCreate;

    public LongColumnManager( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
      this.columnBinary = columnBinary;
      this.primitiveObjectConnector = primitiveObjectConnector;
    }

    private void create() throws IOException{
      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      int decompressSize = compressor.getDecompressSize( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
      byte[] decompressBuffer =  new byte[decompressSize];

      int binaryLength = compressor.decompressAndSet( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength , decompressBuffer );

      byte[] binary = decompressBuffer;
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binaryLength );
      int offset = 0;

      int columnIndexBinaryLength = wrapBuffer.getInt( offset );
      offset += INT_LENGTH;
      int columnIndexBinaryStart = offset;
      offset += columnIndexBinaryLength;

      int dicBinaryLength = wrapBuffer.getInt( offset );
      offset += INT_LENGTH;
      int dicBinaryStart = offset;
      offset += dicBinaryLength;

      IntBuffer indexIntBuffer = BinaryDump.binaryToIntBuffer( decompressBuffer , columnIndexBinaryStart , columnIndexBinaryLength );

      PrimitiveObject[] dicArray = BinaryDump.binaryToLongArray( binary , dicBinaryStart , dicBinaryLength , primitiveObjectConnector );

      IDicManager dicManager = new LongDicManager( dicArray );

      column = new PrimitiveColumn( ColumnType.LONG , columnBinary.columnName );
      column.setCellManager( new BufferDirectDictionaryLinkCellManager( ColumnType.LONG , dicManager , indexIntBuffer ) );

      isCreate = true;
    }

    @Override
    public IColumn get(){
      if( ! isCreate ){
        try{
          create();
        }catch( IOException e ){
          throw new UncheckedIOException( e );
        }
      }
      return column;
    }

    @Override
    public List<String> getColumnKeys(){
      return new ArrayList<String>();
    }

    @Override
    public int getColumnSize(){
      return 0;
    }
  }

}
