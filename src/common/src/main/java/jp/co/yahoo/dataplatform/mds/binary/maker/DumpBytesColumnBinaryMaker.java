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

import jp.co.yahoo.dataplatform.mds.compressor.FindCompressor;
import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;
import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveCell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.IColumnAnalizeResult;

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveType;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.UTF8BytesLinkObj;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class DumpBytesColumnBinaryMaker implements IColumnBinaryMaker{

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column , final MakerCache makerCache ) throws IOException{
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    List<Integer> columnList = new ArrayList<Integer>();
    List<byte[]> objList = new ArrayList<byte[]>();
    objList.add( new byte[0] );
    int totalLength = 0;
    int rowCount = 0;
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      if( cell.getType() == ColumnType.NULL ){
        columnList.add( 0 );
        continue;
      }
      PrimitiveCell byteCell = (PrimitiveCell) cell;
      byte[] obj = byteCell.getRow().getBytes();
      if( obj == null ){
        columnList.add( 0 );
        continue;
      }
      rowCount++;
      totalLength += obj.length;
      objList.add( obj );
      columnList.add( objList.size() - 1 );
    }
    byte[] binaryRaw = convertBinary( columnList , objList , currentConfig , totalLength );
    byte[] binary = currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length );

    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.BYTES , rowCount , binaryRaw.length , totalLength , -1 , binary , 0 , binary.length , null );
  }

  public byte[] convertBinary( final List<Integer> columnList , final List<byte[]> objList , ColumnBinaryMakerConfig currentConfig , final int totalLength ) throws IOException{
    int dicBinarySize = ( objList.size() * Integer.BYTES ) + totalLength;
    int binaryLength = ( Integer.BYTES * 2 ) + ( columnList.size() * Integer.BYTES ) + dicBinarySize;

    byte[] binaryRaw = new byte[binaryLength];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryRaw );
    wrapBuffer.putInt( columnList.size() );
    wrapBuffer.putInt( dicBinarySize );
    for( Integer index : columnList ){
      wrapBuffer.putInt( index );
    }

    for( byte[] obj : objList ){
      wrapBuffer.putInt( obj.length );
      wrapBuffer.put( obj );
    }

    return binaryRaw;
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ){
    int dicBinarySize = ( analizeResult.getRowCount() * Integer.BYTES ) + analizeResult.getLogicalDataSize();
    return ( Integer.BYTES * 2 ) + ( analizeResult.getColumnSize() * Integer.BYTES ) + dicBinarySize;
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
    return new LazyColumn( columnBinary.columnName , columnBinary.columnType , new BytesColumnManager( columnBinary , primitiveObjectConnector ) );
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    loadInMemoryStorage( columnBinary , columnBinary.binaryStart , columnBinary.binaryLength , allocator );
  }

  public void loadInMemoryStorage( final ColumnBinary columnBinary , final int start , final int length , final IMemoryAllocator allocator ) throws IOException{
    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress( columnBinary.binary , start , length );
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary );
    int indexListSize = wrapBuffer.getInt();
    int objBinaryLength = wrapBuffer.getInt();
    int indexBinaryStart = Integer.BYTES * 2;
    int indexBinaryLength = Integer.BYTES * indexListSize;
    int objBinaryStart = indexBinaryStart + indexBinaryLength;

    IntBuffer indexBuffer = ByteBuffer.wrap( binary , indexBinaryStart , indexBinaryLength ).asIntBuffer();
    ByteBuffer dicBuffer = ByteBuffer.wrap( binary , objBinaryStart , objBinaryLength );
    int skipLength = dicBuffer.getInt();
    dicBuffer.position( dicBuffer.position() + skipLength );
    int size = indexBuffer.capacity();
    for( int i = 0 ; i < indexListSize ; i++ ){
      int index = indexBuffer.get();
      if( index != 0 ){
        int objLength = dicBuffer.getInt();
        allocator.setBytes( i , binary , dicBuffer.position() , objLength );
        dicBuffer.position( dicBuffer.position() + objLength );
      }
    }
    allocator.setValueCount( indexListSize );
  }

  @Override
  public void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary ) throws IOException{
    parentNode.getChildNode( columnBinary.columnName ).disable();
  }

  public class BytesDicManager implements IDicManager{

    private final PrimitiveObject[] dicArray;

    public BytesDicManager( final PrimitiveObject[] dicArray ){
      this.dicArray = dicArray;
    }

    @Override
    public PrimitiveObject get( final int index ) throws IOException{
      return dicArray[index];
    }

    @Override
    public int getDicSize() throws IOException{
      return dicArray.length;
    }

  }

  public class BytesColumnManager implements IColumnManager{

    private final IPrimitiveObjectConnector primitiveObjectConnector;
    private final ColumnBinary columnBinary;
    private final int binaryStart;
    private final int binaryLength;
    private PrimitiveColumn column;
    private boolean isCreate;

    public BytesColumnManager( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
      this.columnBinary = columnBinary;
      this.primitiveObjectConnector = primitiveObjectConnector;
      this.binaryStart = columnBinary.binaryStart;
      this.binaryLength = columnBinary.binaryLength;
    }

    public BytesColumnManager( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector , final int binaryStart , final int binaryLength ) throws IOException{
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
      int indexListSize = wrapBuffer.getInt();
      int objBinaryLength = wrapBuffer.getInt();
      int indexBinaryStart = Integer.BYTES * 2;
      int indexBinaryLength = Integer.BYTES * indexListSize;
      int objBinaryStart = indexBinaryStart + indexBinaryLength;

      IntBuffer indexIntBuffer = ByteBuffer.wrap( binary , indexBinaryStart , indexBinaryLength ).asIntBuffer();
      ByteBuffer dicBuffer = ByteBuffer.wrap( binary , objBinaryStart , objBinaryLength );
      PrimitiveObject[] dicArray = new PrimitiveObject[ columnBinary.rowCount + 1 ];
      for( int i = 0 ; i < dicArray.length ; i++ ){
        int objLength = dicBuffer.getInt();
        dicArray[i] = primitiveObjectConnector.convert( PrimitiveType.BYTES , new UTF8BytesLinkObj( binary , dicBuffer.position() , objLength ) );
        dicBuffer.position( dicBuffer.position() + objLength );
      }

      IDicManager dicManager = new BytesDicManager( dicArray );
      column = new PrimitiveColumn( columnBinary.columnType , columnBinary.columnName );
      column.setCellManager( new BufferDirectDictionaryLinkCellManager( ColumnType.BYTES , dicManager , indexIntBuffer ) );
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
