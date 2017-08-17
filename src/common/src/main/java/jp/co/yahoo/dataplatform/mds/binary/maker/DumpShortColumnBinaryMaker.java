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
import java.nio.ShortBuffer;
import java.nio.IntBuffer;

import java.util.List;
import java.util.ArrayList;

import jp.co.yahoo.dataplatform.mds.compressor.FindCompressor;
import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;
import jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength;
import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveCell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;

import jp.co.yahoo.dataplatform.schema.objects.ShortObj;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveType;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.binary.BinaryUtil;
import jp.co.yahoo.dataplatform.mds.binary.BinaryDump;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.SortedIntegerConverter;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class DumpShortColumnBinaryMaker implements IColumnBinaryMaker{

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column , final MakerCache makerCache ) throws IOException{
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    List<Integer> columnList = new ArrayList<Integer>();
    List<Short> objList = new ArrayList<Short>();
    int rowCount = 0;
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      if( cell.getType() == ColumnType.NULL ){
        continue;
      }
      rowCount++;
      PrimitiveCell byteCell = (PrimitiveCell) cell;
      objList.add( Short.valueOf( byteCell.getRow().getShort() ) );
      columnList.add( Integer.valueOf( i ) );
    }
    byte[] binaryRaw = convertBinary( columnList , objList , currentConfig );
    byte[] binary = currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length );

    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.SHORT , rowCount , binaryRaw.length , rowCount * PrimitiveByteLength.SHORT_LENGTH , -1 , binary , 0 , binary.length , null );
  }

  public byte[] convertBinary( final List<Integer> columnList , final List<Short> objList , ColumnBinaryMakerConfig currentConfig ) throws IOException{
    byte[] columnBinaryRaw = BinaryUtil.toLengthBytesBinary( SortedIntegerConverter.getBinary( columnList ) );
    byte[] dicRawBinary = BinaryUtil.toLengthBytesBinary( BinaryDump.dumpShort( objList ) );

    byte[] binaryRaw = new byte[ columnBinaryRaw.length + dicRawBinary.length ];
    int offset = 0;
    System.arraycopy( columnBinaryRaw , 0 , binaryRaw , offset , columnBinaryRaw.length );
    offset += columnBinaryRaw.length;
    System.arraycopy( dicRawBinary , 0 , binaryRaw , offset , dicRawBinary.length );
    return binaryRaw;
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
    return new LazyColumn( columnBinary.columnName , columnBinary.columnType , new ShortColumnManager( columnBinary , primitiveObjectConnector ) );
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    loadInMemoryStorage( columnBinary , columnBinary.binaryStart , columnBinary.binaryLength , allocator );
  }

  public void loadInMemoryStorage( final ColumnBinary columnBinary , final int start , final int length , final IMemoryAllocator allocator ) throws IOException{
    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress( columnBinary.binary , start , length );
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary );
    int offset = 0;
    int columnBinaryLength = wrapBuffer.getInt( offset );
    offset += PrimitiveByteLength.INT_LENGTH;
    int columnBinaryStart = offset;
    offset += columnBinaryLength;

    int dicBinaryLength = wrapBuffer.getInt( offset );
    offset += PrimitiveByteLength.INT_LENGTH;
    int dicBinaryStart = offset;
    offset += dicBinaryLength;

    List<Integer> columnIndexList = SortedIntegerConverter.getIntegerList( binary , columnBinaryStart , columnBinaryLength );
    List<Short> dicList = BinaryDump.binaryToShortList( binary , dicBinaryStart , dicBinaryLength );
    for( int i = 0 ; i < columnIndexList.size() ; i++ ){
      allocator.setShort( columnIndexList.get( i ) , dicList.get( i ) );
    }
    allocator.setValueCount( columnIndexList.get( columnIndexList.size() - 1 ) );
  }

  @Override
  public void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary ) throws IOException{
    parentNode.getChildNode( columnBinary.columnName ).disable();
  }

  public class ShortDicManager implements IDicManager{

    private final IPrimitiveObjectConnector primitiveObjectConnector;
    private final ShortBuffer dicBuffer;
    private final int dicLength;

    public ShortDicManager( final IPrimitiveObjectConnector primitiveObjectConnector , final ShortBuffer dicBuffer ){
      this.primitiveObjectConnector = primitiveObjectConnector;
      this.dicBuffer = dicBuffer;
      dicLength = dicBuffer.capacity();
    }

    @Override
    public PrimitiveObject get( final int index ) throws IOException{
      if( index == 0 ){
        return null;
      }
      return primitiveObjectConnector.convert( PrimitiveType.SHORT , new ShortObj( dicBuffer.get( index - 1 ) ) );
    }

    @Override
    public int getDicSize() throws IOException{
      return dicLength;
    }

  }

  public class ShortColumnManager implements IColumnManager{

    private final IPrimitiveObjectConnector primitiveObjectConnector;
    private final ColumnBinary columnBinary;
    private final int binaryStart;
    private final int binaryLength;
    private PrimitiveColumn column;
    private boolean isCreate;

    public ShortColumnManager( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
      this.columnBinary = columnBinary;
      this.primitiveObjectConnector = primitiveObjectConnector;
      this.binaryStart = columnBinary.binaryStart;
      this.binaryLength = columnBinary.binaryLength;
    }

    public ShortColumnManager( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector , final int binaryStart , final int binaryLength ) throws IOException{
      this.columnBinary = columnBinary;
      this.primitiveObjectConnector = primitiveObjectConnector;
      this.binaryStart = binaryStart;
      this.binaryLength = binaryLength;
    }

    private void create() throws IOException{
      if( isCreate ){
        return;
      }
      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] binary = compressor.decompress( columnBinary.binary , binaryStart , binaryLength );
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary );
      int offset = 0;
      int columnBinaryLength = wrapBuffer.getInt( offset );
      offset += PrimitiveByteLength.INT_LENGTH;
      int columnBinaryStart = offset;
      offset += columnBinaryLength;

      int dicBinaryLength = wrapBuffer.getInt( offset );
      offset += PrimitiveByteLength.INT_LENGTH;
      int dicBinaryStart = offset;
      offset += dicBinaryLength;

      List<Integer> columnIndexList = SortedIntegerConverter.getIntegerList( binary , columnBinaryStart , columnBinaryLength );
      IDicManager dicManager = new ShortDicManager( primitiveObjectConnector ,  ByteBuffer.wrap( binary , dicBinaryStart , dicBinaryLength ).asShortBuffer() );
      IntBuffer indexIntBuffer = IntBuffer.allocate( columnIndexList.get( columnIndexList.size() - 1 ).intValue() + 1 );
      int currentIndex = 0;
      int dicIndex = 1;
      for( Integer index : columnIndexList ){
        if( currentIndex != index.intValue() ){
          currentIndex = index.intValue();
          indexIntBuffer.position( currentIndex );
        }
        indexIntBuffer.put( dicIndex );
        currentIndex++;
        dicIndex++;
      }
      column = new PrimitiveColumn( columnBinary.columnType , columnBinary.columnName );
      column.setCellManager( new BufferDirectDictionaryLinkCellManager( ColumnType.SHORT , dicManager , indexIntBuffer ) );

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
