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
import java.nio.CharBuffer;
import java.nio.IntBuffer;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveType;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;
import jp.co.yahoo.dataplatform.schema.objects.StringObj;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.maker.index.BufferDirectSequentialStringCellIndex;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.compressor.FindCompressor;
import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;
import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveCell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.StringColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class UniqStringColumnBinaryMaker implements IColumnBinaryMaker{

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column ) throws IOException{
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    Map<String,Integer> dicMap = new HashMap<String,Integer>();
    List<Integer> columnIndexList = new ArrayList<Integer>();
    List<char[]> stringList = new ArrayList<char[]>();

    dicMap.put( null , Integer.valueOf(0) );
    stringList.add( new String().toCharArray() );

    int totalLength = 0;
    int logicalTotalLength = 0;
    int rowCount = 0;
    boolean hasNull = false;
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      String targetStr = null;
      int strLength = 0;
      if( cell.getType() != ColumnType.NULL ){
        rowCount++;
        PrimitiveCell stringCell = (PrimitiveCell) cell;
        targetStr = stringCell.getRow().getString();
        if( targetStr != null ){
          strLength = targetStr.length() * Character.BYTES;
          logicalTotalLength += strLength;
        }
      }
      else{
        hasNull = true;
      }
      if( ! dicMap.containsKey( targetStr ) ){
        dicMap.put( targetStr , stringList.size() );
        stringList.add( targetStr.toCharArray() );
        totalLength += strLength;
      }
      columnIndexList.add( dicMap.get( targetStr ) );
    }

    if( ! hasNull && dicMap.size() == 2 ){
      return ConstantColumnBinaryMaker.createColumnBinary( new StringObj( new String( stringList.get(1) ) ) , column.getColumnName() , column.size() );
    }

    int rawSize = ( Integer.BYTES * columnIndexList.size() ) + ( Integer.BYTES + totalLength + ( Integer.BYTES * stringList.size() ) ) + ( Integer.BYTES * 2 );
    byte[] binaryRaw = new byte[ rawSize ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryRaw );

    wrapBuffer.putInt( Integer.BYTES * columnIndexList.size() );
    for( Integer index : columnIndexList ){
      wrapBuffer.putInt( index );
    }
    wrapBuffer.putInt( Integer.BYTES + totalLength + ( Integer.BYTES * stringList.size() ) );
    wrapBuffer.putInt( stringList.size() );
    for( char[] dicCharArray : stringList ){
      wrapBuffer.putInt( dicCharArray.length );
    }
    CharBuffer charBuffer = ByteBuffer.wrap( binaryRaw , wrapBuffer.position() , totalLength ).asCharBuffer();
    for( char[] dicCharArray : stringList ){
      charBuffer.put( dicCharArray );
    }

    byte[] binary = currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length );

    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.STRING , rowCount , binaryRaw.length , logicalTotalLength , dicMap.size() , binary , 0 , binary.length , null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ){
    StringColumnAnalizeResult stringAnalizeResult = (StringColumnAnalizeResult)analizeResult;
    if( analizeResult.getNullCount() == 0 && analizeResult.getUniqCount() == 1 ){
      return stringAnalizeResult.getUniqLogicalDataSize();
    }
    return ( Integer.BYTES * analizeResult.getColumnSize() ) + ( Integer.BYTES + stringAnalizeResult.getUniqLogicalDataSize() + ( Integer.BYTES * analizeResult.getUniqCount() ) ) + ( Integer.BYTES * 2 );
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException{
    return new LazyColumn( columnBinary.columnName , columnBinary.columnType , new StringColumnManager( columnBinary ) );
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] decompressBuffer = compressor.decompress( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );

    ByteBuffer wrapBuffer = ByteBuffer.wrap( decompressBuffer );
    int indexListLength = wrapBuffer.getInt();
    IntBuffer indexIntBuffer = ByteBuffer.wrap( decompressBuffer , wrapBuffer.position() , indexListLength ).asIntBuffer();
    wrapBuffer.position( wrapBuffer.position() + indexListLength );
    int dicTotalLength = wrapBuffer.getInt();
    int dicListSize = wrapBuffer.getInt();
    int dicLengthBinaryLength = dicListSize * Integer.BYTES;
    IntBuffer dicLengthBuffer = ByteBuffer.wrap( decompressBuffer , wrapBuffer.position() , dicLengthBinaryLength ).asIntBuffer();
    wrapBuffer.position( wrapBuffer.position() + dicLengthBinaryLength );
    CharBuffer dicBuffer = ByteBuffer.wrap( decompressBuffer , wrapBuffer.position() , decompressBuffer.length - wrapBuffer.position() ).asCharBuffer();
    String[] dicArray = new String[dicListSize];
    for( int i = 0 ; i < dicArray.length ; i++ ){
      char[] chars = new char[dicLengthBuffer.get()];
      dicBuffer.get( chars );
      dicArray[i] = new String( chars );
    }

    int size = indexIntBuffer.capacity();
    for( int i = 0 ; i < size ; i++ ){
      int dicIndex = indexIntBuffer.get();
      if( dicIndex != 0 ){
        allocator.setString( i , dicArray[dicIndex] );
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
    private PrimitiveColumn column;
    private boolean isCreate;

    public StringColumnManager( final ColumnBinary columnBinary ) throws IOException{
      this.columnBinary = columnBinary;
    }

    private void create() throws IOException{
      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] decompressBuffer = compressor.decompress( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );

      ByteBuffer wrapBuffer = ByteBuffer.wrap( decompressBuffer );
      int indexListLength = wrapBuffer.getInt();
      IntBuffer indexIntBuffer = ByteBuffer.wrap( decompressBuffer , wrapBuffer.position() , indexListLength ).asIntBuffer();
      wrapBuffer.position( wrapBuffer.position() + indexListLength );
      int dicTotalLength = wrapBuffer.getInt();
      int dicListSize = wrapBuffer.getInt();
      int dicLengthBinaryLength = dicListSize * Integer.BYTES;
      IntBuffer dicLengthBuffer = ByteBuffer.wrap( decompressBuffer , wrapBuffer.position() , dicLengthBinaryLength ).asIntBuffer();
      wrapBuffer.position( wrapBuffer.position() + dicLengthBinaryLength );
      CharBuffer dicBuffer = ByteBuffer.wrap( decompressBuffer , wrapBuffer.position() , decompressBuffer.length - wrapBuffer.position() ).asCharBuffer();
      PrimitiveObject[] dicArray = new PrimitiveObject[dicListSize];
      for( int i = 0 ; i < dicArray.length ; i++ ){
        char[] chars = new char[dicLengthBuffer.get()];
        dicBuffer.get( chars );
        dicArray[i] = new StringObj( new String( chars ) );
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
