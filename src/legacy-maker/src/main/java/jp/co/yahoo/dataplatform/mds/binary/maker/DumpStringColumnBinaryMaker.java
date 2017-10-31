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
import jp.co.yahoo.dataplatform.mds.spread.analyzer.StringColumnAnalizeResult;

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveType;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.UTF8BytesLinkObj;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class DumpStringColumnBinaryMaker implements IColumnBinaryMaker{

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column ) throws IOException{
    if( column.size() == 0 ){
      return new UnsupportedColumnBinaryMaker().toBinary( commonConfig , currentConfigNode , column );
    }
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    byte[] nullFlagBytes = new byte[column.size()];
    List<byte[]> objList = new ArrayList<byte[]>();
    int totalLength = 0;
    int logicalDataLength = 0;
    int rowCount = 0;
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      if( cell.getType() == ColumnType.NULL ){
        nullFlagBytes[i] = (byte)1;
        objList.add( new byte[0] );
        continue;
      }
      PrimitiveCell byteCell = (PrimitiveCell) cell;
      String strObj = byteCell.getRow().getString();
      if( strObj == null ){
        nullFlagBytes[i] = (byte)1;
        objList.add( new byte[0] );
        continue;
      }
      byte[] obj = strObj.getBytes( "UTF-8" );
      rowCount++;
      totalLength += obj.length;
      logicalDataLength += strObj.length() * Character.BYTES;
      objList.add( obj );
    }
    byte[] binaryRaw = convertBinary( nullFlagBytes , objList , currentConfig , totalLength );
    byte[] binary = currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length );

    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.STRING , rowCount , binaryRaw.length , logicalDataLength , -1 , binary , 0 , binary.length , null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ){
    StringColumnAnalizeResult stringAnalizeResult = (StringColumnAnalizeResult)analizeResult;
    int dicBinarySize = ( analizeResult.getColumnSize() * Integer.BYTES ) + stringAnalizeResult.getTotalUtf8ByteSize();
    return ( Integer.BYTES * 2 ) + analizeResult.getColumnSize() + dicBinarySize;
  }

  public byte[] convertBinary( final byte[] nullFlagBytes , final List<byte[]> objList , ColumnBinaryMakerConfig currentConfig , final int totalLength ) throws IOException{
    int dicBinarySize = ( objList.size() * Integer.BYTES ) + totalLength;
    int binaryLength = ( Integer.BYTES * 2 ) + nullFlagBytes.length + dicBinarySize;

    byte[] binaryRaw = new byte[binaryLength];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryRaw );
    wrapBuffer.putInt( nullFlagBytes.length );
    wrapBuffer.putInt( dicBinarySize );
    wrapBuffer.put( nullFlagBytes );

    for( byte[] obj : objList ){
      wrapBuffer.putInt( obj.length );
      wrapBuffer.put( obj );
    }

    return binaryRaw;
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException{
    return new LazyColumn( columnBinary.columnName , columnBinary.columnType , new StringColumnManager( columnBinary ) );
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    loadInMemoryStorage( columnBinary , columnBinary.binaryStart , columnBinary.binaryLength , allocator );
  }

  public void loadInMemoryStorage( final ColumnBinary columnBinary , final int start , final int length , final IMemoryAllocator allocator ) throws IOException{
    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress( columnBinary.binary , start , length );
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary );
    int indexBinaryLength = wrapBuffer.getInt();
    int objBinaryLength = wrapBuffer.getInt();
    int indexBinaryStart = Integer.BYTES * 2;
    int objBinaryStart = indexBinaryStart + indexBinaryLength;

    ByteBuffer nullFlagBuffer = ByteBuffer.wrap( binary , indexBinaryStart , indexBinaryLength );
    ByteBuffer dicBuffer = ByteBuffer.wrap( binary , objBinaryStart , objBinaryLength );
    for( int i = 0 ; i < indexBinaryLength ; i++ ){
      byte nullFlag = nullFlagBuffer.get();
      int objLength = dicBuffer.getInt();
      if( nullFlag == (byte)0 ){
        allocator.setBytes( i , binary , dicBuffer.position() , objLength );
        dicBuffer.position( dicBuffer.position() + objLength );
      }
      else{
        allocator.setNull( i );
      }
    }
    allocator.setValueCount( indexBinaryLength );
  }

  @Override
  public void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary , final int spreadIndex ) throws IOException{
    parentNode.getChildNode( columnBinary.columnName ).disable();
  }

  public class StringDicManager implements IDicManager{

    private final PrimitiveObject[] dicArray;

    public StringDicManager( final PrimitiveObject[] dicArray ){
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

  public class StringColumnManager implements IColumnManager{

    private final ColumnBinary columnBinary;
    private final int binaryStart;
    private final int binaryLength;
    private PrimitiveColumn column;
    private boolean isCreate;

    public StringColumnManager( final ColumnBinary columnBinary ) throws IOException{
      this.columnBinary = columnBinary;
      this.binaryStart = columnBinary.binaryStart;
      this.binaryLength = columnBinary.binaryLength;
    }

    public StringColumnManager( final ColumnBinary columnBinary , final int binaryStart , final int binaryLength ) throws IOException{
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
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary );
      int indexBinaryLength = wrapBuffer.getInt();
      int objBinaryLength = wrapBuffer.getInt();
      int indexBinaryStart = Integer.BYTES * 2;
      int objBinaryStart = indexBinaryStart + indexBinaryLength;

      ByteBuffer nullFlagBuffer = ByteBuffer.wrap( binary , indexBinaryStart , indexBinaryLength );
      ByteBuffer dicBuffer = ByteBuffer.wrap( binary , objBinaryStart , objBinaryLength );
      PrimitiveObject[] dicArray = new PrimitiveObject[indexBinaryLength];
      for( int i = 0 ; i < dicArray.length ; i++ ){
        byte nullFlag = nullFlagBuffer.get();
        int objLength = dicBuffer.getInt();
        if( nullFlag == (byte)0 ){
          dicArray[i] = new UTF8BytesLinkObj( binary , dicBuffer.position() , objLength );
          dicBuffer.position( dicBuffer.position() + objLength );
        }
      }

      IDicManager dicManager = new StringDicManager( dicArray );
      column = new PrimitiveColumn( columnBinary.columnType , columnBinary.columnName );
      column.setCellManager( new BufferDirectCellManager( ColumnType.STRING , dicManager , indexBinaryLength ) );
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
