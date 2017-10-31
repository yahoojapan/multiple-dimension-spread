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

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveType;
import jp.co.yahoo.dataplatform.schema.objects.StringObj;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.binary.UTF8BytesLinkObj;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.maker.index.BufferDirectSequentialStringCellIndex;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.compressor.FindCompressor;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveCell;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.StringColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;

public class UniqStringToUTF8BytesColumnBinaryMaker implements IColumnBinaryMaker{

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column ) throws IOException{
    if( column.size() == 0 ){
      return new UnsupportedColumnBinaryMaker().toBinary( commonConfig , currentConfigNode , column );
    }
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    Map<String,Integer> dicMap = new HashMap<String,Integer>();
    List<Integer> columnIndexList = new ArrayList<Integer>( column.size() );
    List<byte[]> stringList = new ArrayList<byte[]>();

    dicMap.put( null , Integer.valueOf(0) );
    stringList.add( new byte[0] );

    int totalLength = 0;
    int logicalTotalLength = 0;
    int rowCount = 0;
    int columnSize = column.size();
    boolean hasNull = false;
    for( int i = 0 ; i < columnSize ; i++ ){
      ICell cell = column.get(i);
      String targetStr = null;
      if( cell.getType() != ColumnType.NULL ){
        rowCount++;
        PrimitiveCell stringCell = (PrimitiveCell) cell;
        targetStr = stringCell.getRow().getString();
        if( targetStr != null ){
          logicalTotalLength += targetStr.length() * Character.BYTES;
        }
      }
      else{
        hasNull = true;
      }
      if( ! dicMap.containsKey( targetStr ) ){
        dicMap.put( targetStr , stringList.size() );
        byte[] stringBytes = targetStr.getBytes( "UTF-8" );
        stringList.add( stringBytes );
        totalLength += stringBytes.length;
      }
      columnIndexList.add( dicMap.get( targetStr ) );
    }

    if( ! hasNull && dicMap.size() == 2 ){                                                                                     return ConstantColumnBinaryMaker.createColumnBinary( new StringObj( new String( stringList.get(1) , "UTF-8" ) ) , column.getColumnName() , column.size() );
    }

    int rawSize = ( Integer.BYTES * columnIndexList.size() ) + ( totalLength + ( Integer.BYTES * stringList.size() ) ) + ( Integer.BYTES * 2 );
    byte[] binaryRaw = new byte[ rawSize ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryRaw );

    wrapBuffer.putInt( Integer.BYTES * columnIndexList.size() );
    for( Integer index : columnIndexList ){
      wrapBuffer.putInt( index );
    }
    wrapBuffer.putInt( totalLength + ( Integer.BYTES * stringList.size() ) );
    for( byte[] dicByteArray : stringList ){
      wrapBuffer.putInt( dicByteArray.length );
      wrapBuffer.put( dicByteArray );
    }

    byte[] binary = currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length );

    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.STRING , rowCount , binaryRaw.length , logicalTotalLength , dicMap.size() , binary , 0 , binary.length , null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ){
    StringColumnAnalizeResult stringAnalizeResult = (StringColumnAnalizeResult)analizeResult;
    if( analizeResult.getNullCount() == 0 && analizeResult.getUniqCount() == 1 ){
      return stringAnalizeResult.getUniqUtf8ByteSize();
    }
    return ( Integer.BYTES * analizeResult.getColumnSize() ) + ( Integer.BYTES + stringAnalizeResult.getUniqUtf8ByteSize() + ( Integer.BYTES * analizeResult.getUniqCount() ) ) + ( Integer.BYTES * 2 );
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException{
    return new LazyColumn( columnBinary.columnName , columnBinary.columnType , new StringColumnManager( columnBinary ) );
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    loadInMemoryStorage( columnBinary , allocator , columnBinary.binaryStart , columnBinary.binaryLength );
  }

  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator , final int columnBinaryStart , final int columnBinaryLength ) throws IOException{
    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] decompressBuffer = compressor.decompress( columnBinary.binary , columnBinaryStart , columnBinaryLength );

    ByteBuffer wrapBuffer = ByteBuffer.wrap( decompressBuffer );
    int indexListLength = wrapBuffer.getInt();
    IntBuffer indexIntBuffer = ByteBuffer.wrap( decompressBuffer , wrapBuffer.position() , indexListLength ).asIntBuffer();
    wrapBuffer.position( wrapBuffer.position() + indexListLength );
    int valueLength = wrapBuffer.getInt();

    int[] dicStartOffset = new int[columnBinary.cardinality];
    int[] dicLengthOffset = new int[columnBinary.cardinality];
    for( int i = 0 ; i < columnBinary.cardinality ; i++ ){
      int byteArrayLength = wrapBuffer.getInt();
      dicStartOffset[i] = wrapBuffer.position();
      dicLengthOffset[i] = byteArrayLength;
      wrapBuffer.position( wrapBuffer.position() + byteArrayLength );
    }

    int size = indexIntBuffer.capacity();
    for( int i = 0 ; i < size ; i++ ){
      int dicIndex = indexIntBuffer.get();
      if( dicIndex != 0 ){
        allocator.setBytes( i , decompressBuffer , dicStartOffset[dicIndex] , dicLengthOffset[dicIndex] );
      }
    }
    allocator.setValueCount( size );
  }

  @Override
  public void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary , final int spreadIndex ) throws IOException{
    parentNode.getChildNode( columnBinary.columnName ).disable();
  }

  public class StringDicManager implements IDicManager{

    private final PrimitiveObject[] dicArray;

    public StringDicManager( final PrimitiveObject[] dicArray ) throws IOException{
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
    private final int columnBinaryStart;
    private final int columnBinaryLength;
    private PrimitiveColumn column;
    private boolean isCreate;

    public StringColumnManager( final ColumnBinary columnBinary ) throws IOException{
      this.columnBinary = columnBinary;
      this.columnBinaryStart = columnBinary.binaryStart;
      this.columnBinaryLength = columnBinary.binaryLength;
    }

    public StringColumnManager( final ColumnBinary columnBinary , final int columnBinaryStart , final int columnBinaryLength ) throws IOException{
      this.columnBinary = columnBinary;
      this.columnBinaryStart = columnBinaryStart;
      this.columnBinaryLength = columnBinaryLength;
    }

    private void create() throws IOException{
      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] decompressBuffer = compressor.decompress( columnBinary.binary , columnBinaryStart , columnBinaryLength );

      ByteBuffer wrapBuffer = ByteBuffer.wrap( decompressBuffer );
      int indexListLength = wrapBuffer.getInt();
      IntBuffer indexIntBuffer = ByteBuffer.wrap( decompressBuffer , wrapBuffer.position() , indexListLength ).asIntBuffer();
      wrapBuffer.position( wrapBuffer.position() + indexListLength );
      int valueLength = wrapBuffer.getInt();

      PrimitiveObject[] dicArray = new PrimitiveObject[columnBinary.cardinality];
      for( int i = 0 ; i < columnBinary.cardinality ; i++ ){
        int byteArrayLength = wrapBuffer.getInt();
        dicArray[i] = new UTF8BytesLinkObj( decompressBuffer , wrapBuffer.position() , byteArrayLength );
        wrapBuffer.position( wrapBuffer.position() + byteArrayLength );
      }

      IDicManager dicManager = new StringDicManager( dicArray );

      column = new PrimitiveColumn( ColumnType.STRING , columnBinary.columnName );
      column.setCellManager( new BufferDirectDictionaryLinkCellManager( ColumnType.STRING , dicManager , indexIntBuffer ) );
      column.setIndex( new BufferDirectSequentialStringCellIndex( dicManager , indexIntBuffer ) );

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
