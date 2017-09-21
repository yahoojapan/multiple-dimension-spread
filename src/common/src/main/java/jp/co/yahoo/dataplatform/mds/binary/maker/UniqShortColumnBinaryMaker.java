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
import java.nio.ShortBuffer;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import jp.co.yahoo.dataplatform.schema.objects.ShortObj;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveType;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.compressor.FindCompressor;
import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveCell;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.maker.index.BufferDirectSequentialNumberCellIndex;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class UniqShortColumnBinaryMaker implements IColumnBinaryMaker{

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column , final MakerCache makerCache ) throws IOException{
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    Map<Short,Integer> dicMap = new HashMap<Short,Integer>();
    int columnIndexLength = column.size() * Integer.BYTES;
    int dicBufferSize = ( column.size() + 1 ) * Short.BYTES;
    byte[] binaryRaw = new byte[ Integer.BYTES * 2 + columnIndexLength + dicBufferSize ];
    ByteBuffer indexWrapBuffer = ByteBuffer.wrap( binaryRaw , 0 , Integer.BYTES + columnIndexLength );
    ByteBuffer dicLengthBuffer = ByteBuffer.wrap( binaryRaw , Integer.BYTES + columnIndexLength , Integer.BYTES );
    ByteBuffer dicWrapBuffer = ByteBuffer.wrap( binaryRaw , Integer.BYTES * 2 + columnIndexLength , dicBufferSize );
    indexWrapBuffer.putInt( columnIndexLength );

    dicMap.put( null , Integer.valueOf(0) );
    dicWrapBuffer.putShort( Short.valueOf( (short)0 ) );

    int rowCount = 0;
    boolean hasNull = false;
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      Short target = null;
      if( cell.getType() != ColumnType.NULL ){
        rowCount++;
        PrimitiveCell stringCell = (PrimitiveCell) cell;
        target = Short.valueOf( stringCell.getRow().getShort() );
      }
      else{
        hasNull = true;
      }
      if( ! dicMap.containsKey( target ) ){
        dicMap.put( target , dicMap.size() );
        dicWrapBuffer.putShort( target.shortValue() );
      }
      indexWrapBuffer.putInt( dicMap.get( target ) );
    }

    if( ! hasNull && dicMap.size() == 2 ){
      ByteBuffer dicBuffer = ByteBuffer.wrap( binaryRaw , Integer.BYTES * 2 + columnIndexLength , dicBufferSize );
      dicBuffer.getShort();
      return ConstantColumnBinaryMaker.createColumnBinary( new ShortObj( dicBuffer.getShort() ) , column.getColumnName() , column.size() );
    }

    int dicLength = dicMap.size() * Short.BYTES;
    int dataLength = binaryRaw.length - ( dicBufferSize - dicLength );
    dicLengthBuffer.putInt( dicLength );
    byte[] binary = currentConfig.compressorClass.compress( binaryRaw , 0 , dataLength );

    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.SHORT , rowCount , dataLength , rowCount * Short.BYTES , dicMap.size() , binary , 0 , binary.length , null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ){
    if( analizeResult.getNullCount() == 0 && analizeResult.getUniqCount() == 1 ){
      return Short.BYTES;
    }
    int columnIndexLength = analizeResult.getColumnSize() * Integer.BYTES;
    int dicSize = ( analizeResult.getUniqCount() + 1 ) * Short.BYTES;
    return Integer.BYTES * 2 + columnIndexLength + dicSize;
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
    return new LazyColumn( columnBinary.columnName , columnBinary.columnType , new ShortColumnManager( columnBinary , primitiveObjectConnector ) );
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    loadInMemoryStorage( columnBinary , allocator , columnBinary.binaryStart , columnBinary.binaryLength );
  }

  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator , final int columnBinaryStart , final int columnBinaryLength ) throws IOException{
    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] decompressBuffer = compressor.decompress( columnBinary.binary , columnBinaryStart , columnBinaryLength );
    ByteBuffer wrapBuffer = ByteBuffer.wrap( decompressBuffer , 0 , decompressBuffer.length );
    int indexListSize = wrapBuffer.getInt();
    IntBuffer indexIntBuffer = ByteBuffer.wrap( decompressBuffer , Integer.BYTES , indexListSize ).asIntBuffer();

    wrapBuffer.position( Integer.BYTES + indexListSize );
    int dicSize = wrapBuffer.getInt();
    int dicStart = Integer.BYTES * 2 + indexListSize;
    ShortBuffer dicBuffer = ByteBuffer.wrap( decompressBuffer , dicStart , dicSize ).asShortBuffer();
    int size = indexIntBuffer.capacity();
    for( int i = 0 ; i < size ; i++ ){
      int dicIndex = indexIntBuffer.get();
      if( dicIndex != 0 ){
        allocator.setShort( i , dicBuffer.get( dicIndex ) );
      }
    }
    allocator.setValueCount( size );
  }

  @Override
  public void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary ) throws IOException{
    parentNode.getChildNode( columnBinary.columnName ).disable();
  }

  public class ShortDicManager implements IDicManager{

    private final PrimitiveObject[] dicArray;
    private final int dicSize;

    public ShortDicManager( final PrimitiveObject[] dicArray ) throws IOException{
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

  public class ShortColumnManager implements IColumnManager{

    private final IPrimitiveObjectConnector primitiveObjectConnector;
    private final ColumnBinary columnBinary;
    private final int columnBinaryStart;
    private final int columnBinaryLength;
    private PrimitiveColumn column;
    private boolean isCreate;

    public ShortColumnManager( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
      this.columnBinary = columnBinary;
      this.primitiveObjectConnector = primitiveObjectConnector;
      this.columnBinaryStart = columnBinary.binaryStart;
      this.columnBinaryLength = columnBinary.binaryLength;
    }

    public ShortColumnManager( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector , final int columnBinaryStart , final int columnBinaryLength ) throws IOException{
      this.columnBinary = columnBinary;
      this.primitiveObjectConnector = primitiveObjectConnector;
      this.columnBinaryStart = columnBinaryStart;
      this.columnBinaryLength = columnBinaryLength;
    }

    private void create() throws IOException{
      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] decompressBuffer = compressor.decompress( columnBinary.binary , columnBinaryStart , columnBinaryLength );
      ByteBuffer wrapBuffer = ByteBuffer.wrap( decompressBuffer , 0 , decompressBuffer.length );
      int indexListSize = wrapBuffer.getInt();
      IntBuffer indexIntBuffer = ByteBuffer.wrap( decompressBuffer , Integer.BYTES , indexListSize ).asIntBuffer();

      wrapBuffer.position( Integer.BYTES + indexListSize );
      int dicSize = wrapBuffer.getInt();
      int dicStart = Integer.BYTES * 2 + indexListSize;
      ShortBuffer dicBuffer = ByteBuffer.wrap( decompressBuffer , dicStart , dicSize ).asShortBuffer();
      dicBuffer.get();
      PrimitiveObject[] dicArray = new PrimitiveObject[dicSize/Short.BYTES];
      for( int i = 1 ; i < dicArray.length ; i++ ){
        dicArray[i] = primitiveObjectConnector.convert( PrimitiveType.SHORT , new ShortObj( dicBuffer.get() ) );
      }

      IDicManager dicManager = new ShortDicManager( dicArray );

      column = new PrimitiveColumn( ColumnType.SHORT , columnBinary.columnName );
      column.setCellManager( new BufferDirectDictionaryLinkCellManager( ColumnType.SHORT , dicManager , indexIntBuffer ) );
      column.setIndex( new BufferDirectSequentialNumberCellIndex( ColumnType.SHORT , dicManager , indexIntBuffer ) );

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
