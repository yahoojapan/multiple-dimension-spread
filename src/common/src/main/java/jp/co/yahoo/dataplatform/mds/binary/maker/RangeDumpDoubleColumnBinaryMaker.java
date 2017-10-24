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
import java.nio.DoubleBuffer;

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

import jp.co.yahoo.dataplatform.schema.objects.DoubleObj;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveType;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.maker.index.RangeDoubleIndex;
import jp.co.yahoo.dataplatform.mds.binary.maker.index.SequentialNumberCellIndex;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.blockindex.DoubleRangeBlockIndex;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class RangeDumpDoubleColumnBinaryMaker extends DumpDoubleColumnBinaryMaker{

  private static final int HEADER_SIZE = ( Double.BYTES * 2 ) + Integer.BYTES;

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column ) throws IOException{
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    byte[] parentsBinaryRaw = new byte[ ( Integer.BYTES * 2 ) + column.size() + ( column.size() * Double.BYTES ) ];
    ByteBuffer lengthBuffer = ByteBuffer.wrap( parentsBinaryRaw );
    lengthBuffer.putInt( column.size() );
    lengthBuffer.putInt( column.size() * Double.BYTES );

    ByteBuffer nullFlagBuffer = ByteBuffer.wrap( parentsBinaryRaw , Integer.BYTES * 2 , column.size() );
    DoubleBuffer doubleBuffer = ByteBuffer.wrap( parentsBinaryRaw , ( Integer.BYTES * 2 + column.size() ) , ( column.size() * Double.BYTES ) ).asDoubleBuffer();
    int rowCount = 0;
    boolean hasNull = false;
    Double min = Double.MAX_VALUE;
    Double max = Double.MIN_VALUE;
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      if( cell.getType() == ColumnType.NULL ){
        nullFlagBuffer.put( (byte)1 );
        doubleBuffer.put( (double)0 );
        hasNull = true;
      }
      else{
        rowCount++;
        PrimitiveCell byteCell = (PrimitiveCell) cell;
        nullFlagBuffer.put( (byte)0 );
        Double target = Double.valueOf( byteCell.getRow().getDouble() );
        doubleBuffer.put( target );
        if( 0 < min.compareTo( target ) ){
          min = Double.valueOf( target );
        }
        if( max.compareTo( target ) < 0 ){
          max = Double.valueOf( target );
        }
      }
    }

    if( ! hasNull && min.equals( max ) ){
      return ConstantColumnBinaryMaker.createColumnBinary( new DoubleObj( min ) , column.getColumnName() , column.size() );
    }

    byte[] binary;
    int rawLength;
    if( hasNull ){
      byte[] binaryRaw = parentsBinaryRaw;
      byte[] compressBinaryRaw = currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length );
      rawLength = binaryRaw.length;
      
      binary = new byte[ HEADER_SIZE + compressBinaryRaw.length ];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary );
      wrapBuffer.putDouble( min );
      wrapBuffer.putDouble( max );
      wrapBuffer.putInt( 0 );
      wrapBuffer.put( compressBinaryRaw );
    }
    else{
      rawLength = column.size() * Double.BYTES;
      byte[] compressBinaryRaw = currentConfig.compressorClass.compress( parentsBinaryRaw , ( Integer.BYTES * 2 ) + column.size() , column.size() * Double.BYTES );

      binary = new byte[ HEADER_SIZE + compressBinaryRaw.length ];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary );
      wrapBuffer.putDouble( min );
      wrapBuffer.putDouble( max );
      wrapBuffer.putInt( 1 );
      wrapBuffer.put( compressBinaryRaw );
    }
    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.DOUBLE , rowCount , rawLength , rowCount * Double.BYTES , -1 , binary , 0 , binary.length , null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ){
    if( analizeResult.getNullCount() == 0 && analizeResult.getUniqCount() == 1 ){
      return Double.BYTES;
    }
    else if( analizeResult.getNullCount() == 0 ){
      return analizeResult.getColumnSize() * Double.BYTES;
    }
    else{
      return super.calcBinarySize( analizeResult );
    }
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Double min = Double.valueOf( wrapBuffer.getDouble() );
    Double max = Double.valueOf( wrapBuffer.getDouble() );
    int type = wrapBuffer.getInt();
    if( type == 0 ){
      return new HeaderIndexLazyColumn( 
        columnBinary.columnName , 
        columnBinary.columnType , 
        new DoubleColumnManager( 
          columnBinary , 
          columnBinary.binaryStart + HEADER_SIZE , 
          columnBinary.binaryLength - HEADER_SIZE ) , 
          new RangeDoubleIndex( min , max ) 
      );
    }
    else{
      return new HeaderIndexLazyColumn(
        columnBinary.columnName ,
        columnBinary.columnType ,
        new RangeDoubleColumnManager(
          columnBinary ,
          columnBinary.binaryStart + HEADER_SIZE ,
          columnBinary.binaryLength - HEADER_SIZE ) ,
          new RangeDoubleIndex( min , max )
      );
    }
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    wrapBuffer.position( Double.BYTES * 2 );
    int type = wrapBuffer.getInt();
    if( type == 0 ){
      loadInMemoryStorage( columnBinary , columnBinary.binaryStart + HEADER_SIZE , columnBinary.binaryLength - HEADER_SIZE , allocator );
      return;
    }
    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress( columnBinary.binary , columnBinary.binaryStart + HEADER_SIZE , columnBinary.binaryLength - HEADER_SIZE );
    wrapBuffer = wrapBuffer.wrap( binary );
    for( int i = 0 ; i < columnBinary.rowCount ; i++ ){
      allocator.setDouble( i , wrapBuffer.getDouble() );
    }
    allocator.setValueCount( columnBinary.rowCount );
  }

  @Override
  public void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary , final int spreadIndex ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Double min = Double.valueOf( wrapBuffer.getDouble() );
    Double max = Double.valueOf( wrapBuffer.getDouble() );
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new DoubleRangeBlockIndex( min , max ) );
  }

  public class RangeDoubleDicManager implements IDicManager{

    private final DoubleBuffer dicBuffer;
    private final int dicLength;

    public RangeDoubleDicManager( final DoubleBuffer dicBuffer ){
      this.dicBuffer = dicBuffer;
      dicLength = dicBuffer.capacity(); 
    }

    @Override
    public PrimitiveObject get( final int index ) throws IOException{
      return new DoubleObj( dicBuffer.get( index ) );
    }

    @Override
    public int getDicSize() throws IOException{
      return dicLength;
    }

  }

  public class RangeDoubleColumnManager implements IColumnManager{

    private final ColumnBinary columnBinary;
    private final int binaryStart;
    private final int binaryLength;
    private PrimitiveColumn column;
    private boolean isCreate;

    public RangeDoubleColumnManager( final ColumnBinary columnBinary , final int binaryStart , final int binaryLength ) throws IOException{
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
      IDicManager dicManager = new RangeDoubleDicManager( ByteBuffer.wrap( binary ).asDoubleBuffer() );
      column.setCellManager( new BufferDirectCellManager( columnBinary.columnType , dicManager , columnBinary.rowCount ) );
      column.setIndex( new SequentialNumberCellIndex( ColumnType.DOUBLE , dicManager ) );

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
