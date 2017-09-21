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
import java.nio.LongBuffer;

import java.util.List;
import java.util.ArrayList;

import jp.co.yahoo.dataplatform.mds.compressor.FindCompressor;
import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;
import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveCell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.IColumnAnalizeResult;

import jp.co.yahoo.dataplatform.schema.objects.LongObj;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveType;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.maker.index.RangeLongIndex;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.blockindex.LongRangeBlockIndex;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class RangeDumpLongColumnBinaryMaker extends DumpLongColumnBinaryMaker{

  private static final int HEADER_SIZE = ( Long.BYTES * 2 ) + Integer.BYTES;

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column , final MakerCache makerCache ) throws IOException{
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    byte[] parentsBinaryRaw = new byte[ ( Integer.BYTES * 2 ) + column.size() + ( column.size() * Long.BYTES ) ];
    ByteBuffer lengthBuffer = ByteBuffer.wrap( parentsBinaryRaw );
    lengthBuffer.putInt( column.size() );
    lengthBuffer.putInt( column.size() * Long.BYTES );

    ByteBuffer nullFlagBuffer = ByteBuffer.wrap( parentsBinaryRaw , Integer.BYTES * 2 , column.size() );
    LongBuffer longBuffer = ByteBuffer.wrap( parentsBinaryRaw , ( Integer.BYTES * 2 + column.size() ) , ( column.size() * Long.BYTES ) ).asLongBuffer();
    int rowCount = 0;
    boolean hasNull = false;
    Long min = Long.MAX_VALUE;
    Long max = Long.MIN_VALUE;
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      if( cell.getType() == ColumnType.NULL ){
        nullFlagBuffer.put( (byte)1 );
        longBuffer.put( (long)0 );
        hasNull = true;
      }
      else{
        rowCount++;
        PrimitiveCell byteCell = (PrimitiveCell) cell;
        nullFlagBuffer.put( (byte)0 );
        Long target = Long.valueOf( byteCell.getRow().getLong() );
        longBuffer.put( target );
        if( 0 < min.compareTo( target ) ){
          min = Long.valueOf( target );
        }
        if( max.compareTo( target ) < 0 ){
          max = Long.valueOf( target );
        }
      }
    }

    if( ! hasNull && min.equals( max ) ){
      return ConstantColumnBinaryMaker.createColumnBinary( new LongObj( min ) , column.getColumnName() , column.size() );
    }

    byte[] binary;
    int rawLength;
    if( hasNull ){
      byte[] binaryRaw = parentsBinaryRaw;
      byte[] compressBinaryRaw = currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length );
      rawLength = binaryRaw.length;
      
      binary = new byte[ HEADER_SIZE + compressBinaryRaw.length ];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary );
      wrapBuffer.putLong( min );
      wrapBuffer.putLong( max );
      wrapBuffer.putInt( 0 );
      wrapBuffer.put( compressBinaryRaw );
    }
    else{
      rawLength = column.size() * Long.BYTES;
      byte[] compressBinaryRaw = currentConfig.compressorClass.compress( parentsBinaryRaw , ( Integer.BYTES * 2 ) + column.size() , column.size() * Long.BYTES );

      binary = new byte[ HEADER_SIZE + compressBinaryRaw.length ];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary );
      wrapBuffer.putLong( min );
      wrapBuffer.putLong( max );
      wrapBuffer.putInt( 1 );
      wrapBuffer.put( compressBinaryRaw );
    }
    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.LONG , rowCount , rawLength , rowCount * Long.BYTES , -1 , binary , 0 , binary.length , null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ){
    if( analizeResult.getNullCount() == 0 && analizeResult.getUniqCount() == 1 ){
      return Long.BYTES;
    }
    else if( analizeResult.getNullCount() == 0 ){
      return analizeResult.getColumnSize() * Long.BYTES;
    }
    else{
      return super.calcBinarySize( analizeResult );
    }
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Long min = Long.valueOf( wrapBuffer.getLong() );
    Long max = Long.valueOf( wrapBuffer.getLong() );
    int type = wrapBuffer.getInt();
    if( type == 0 ){
      return new HeaderIndexLazyColumn( 
        columnBinary.columnName , 
        columnBinary.columnType , 
        new LongColumnManager( 
          columnBinary , 
          columnBinary.binaryStart + HEADER_SIZE , 
          columnBinary.binaryLength - HEADER_SIZE ) , 
          new RangeLongIndex( min , max ) 
      );
    }
    else{
      return new HeaderIndexLazyColumn(
        columnBinary.columnName ,
        columnBinary.columnType ,
        new RangeLongColumnManager(
          columnBinary ,
          columnBinary.binaryStart + HEADER_SIZE ,
          columnBinary.binaryLength - HEADER_SIZE ) ,
          new RangeLongIndex( min , max )
      );
    }
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    wrapBuffer.position( Long.BYTES * 2 );
    int type = wrapBuffer.getInt();
    if( type == 0 ){
      loadInMemoryStorage( columnBinary , columnBinary.binaryStart + HEADER_SIZE , columnBinary.binaryLength - HEADER_SIZE , allocator );
      return;
    }
    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress( columnBinary.binary , columnBinary.binaryStart + HEADER_SIZE , columnBinary.binaryLength - HEADER_SIZE );
    wrapBuffer = wrapBuffer.wrap( binary );
    for( int i = 0 ; i < columnBinary.rowCount ; i++ ){
      allocator.setLong( i , wrapBuffer.getLong() );
    }
    allocator.setValueCount( columnBinary.rowCount );
  }

  @Override
  public void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Long min = Long.valueOf( wrapBuffer.getLong() );
    Long max = Long.valueOf( wrapBuffer.getLong() );
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new LongRangeBlockIndex( min , max ) );
  }

  public class RangeLongDicManager implements IDicManager{

    private final LongBuffer dicBuffer;
    private final int dicLength;

    public RangeLongDicManager( final LongBuffer dicBuffer ){
      this.dicBuffer = dicBuffer;
      dicLength = dicBuffer.capacity(); 
    }

    @Override
    public PrimitiveObject get( final int index ) throws IOException{
      return new LongObj( dicBuffer.get( index ) );
    }

    @Override
    public int getDicSize() throws IOException{
      return dicLength;
    }

  }

  public class RangeLongColumnManager implements IColumnManager{

    private final ColumnBinary columnBinary;
    private final int binaryStart;
    private final int binaryLength;
    private PrimitiveColumn column;
    private boolean isCreate;

    public RangeLongColumnManager( final ColumnBinary columnBinary , final int binaryStart , final int binaryLength ) throws IOException{
      this.columnBinary = columnBinary;
      this.binaryStart = binaryStart;
      this.binaryLength = binaryLength;
    }

    private void create() throws IOException{
      if( isCreate ){
        return;
      }
      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] binary = compressor.decompress( columnBinary.binary , binaryStart , binaryLength );

      column = new PrimitiveColumn( columnBinary.columnType , columnBinary.columnName );
      IDicManager dicManager = new RangeLongDicManager( ByteBuffer.wrap( binary ).asLongBuffer() );
      column.setCellManager( new BufferDirectCellManager( ColumnType.LONG , dicManager , columnBinary.rowCount ) );

      isCreate = true;
    }

    @Override
    public IColumn get(){
      try{
        create();
      }catch( IOException e ){
        throw new UncheckedIOException( e );
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
