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

import jp.co.yahoo.dataplatform.schema.objects.StringObj;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveType;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.UTF8BytesLinkObj;
import jp.co.yahoo.dataplatform.mds.binary.maker.index.RangeStringIndex;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.blockindex.StringRangeBlockIndex;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class RangeDumpStringColumnBinaryMaker extends DumpStringColumnBinaryMaker{

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column , final MakerCache makerCache ) throws IOException{

    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }

    byte[] nullFlagBytes = new byte[column.size()];
    List<byte[]> objList = new ArrayList<byte[]>();
    int totalLength = 0;
    int logicalDataLength = 0;
    int rowCount = 0;
    boolean hasNull = false;
    String min = null;
    String max = "";
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      if( cell.getType() == ColumnType.NULL ){
        nullFlagBytes[i] = (byte)1;
        objList.add( new byte[0] );
        hasNull = true;
        continue;
      }
      PrimitiveCell byteCell = (PrimitiveCell) cell;
      String strObj = byteCell.getRow().getString();
      if( strObj == null ){
        nullFlagBytes[i] = (byte)1;
        objList.add( new byte[0] );
        hasNull = true;
        continue;
      }
      byte[] obj = strObj.getBytes( "UTF-8" );
      rowCount++;
      totalLength += obj.length;
      logicalDataLength += strObj.length() * Character.BYTES;
      objList.add( obj );
      if( max.compareTo( strObj ) < 0 ){
        max = strObj;
      }
      if( min == null || 0 < min.compareTo( strObj ) ){
        min = strObj;
      }
    }

    if( ! hasNull && min.equals( max ) ){
      return ConstantColumnBinaryMaker.createColumnBinary( new StringObj( min ) , column.getColumnName() , column.size() );
    }

    byte[] binary;
    int rawLength;

    int minLength = Character.BYTES * min.length();
    int maxLength = Character.BYTES * max.length();
    int headerSize = Integer.BYTES + minLength + Integer.BYTES + maxLength + Integer.BYTES;

    if( hasNull ){
      byte[] binaryRaw = convertBinary( nullFlagBytes , objList , currentConfig , totalLength );
      byte[] compressBinaryRaw = currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length );
      rawLength = binaryRaw.length;
      
      binary = new byte[ headerSize + compressBinaryRaw.length ];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary );
      wrapBuffer.putInt( minLength );
      wrapBuffer.asCharBuffer().put( min );
      wrapBuffer.position( wrapBuffer.position() + minLength );
      wrapBuffer.putInt( maxLength );
      wrapBuffer.asCharBuffer().put( max );
      wrapBuffer.position( wrapBuffer.position() + maxLength );
      wrapBuffer.putInt( 0 );
      wrapBuffer.put( compressBinaryRaw );
    }
    else{
      rawLength = totalLength + Integer.BYTES * objList.size();
      byte[] binaryRaw = new byte[rawLength];
      ByteBuffer objBuffer = ByteBuffer.wrap( binaryRaw );
      for( byte[] obj : objList ){
        objBuffer.putInt( obj.length );
        objBuffer.put( obj );
      }
      byte[] compressBinaryRaw = currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length );

      binary = new byte[ headerSize + compressBinaryRaw.length ];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary );
      wrapBuffer.putInt( minLength );
      wrapBuffer.asCharBuffer().put( min );
      wrapBuffer.position( wrapBuffer.position() + minLength );
      wrapBuffer.putInt( maxLength );
      wrapBuffer.asCharBuffer().put( max );
      wrapBuffer.position( wrapBuffer.position() + maxLength );
      wrapBuffer.putInt( 1 );
      wrapBuffer.put( compressBinaryRaw );
    }
    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.STRING , rowCount , rawLength , logicalDataLength , -1 , binary , 0 , binary.length , null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ){
    StringColumnAnalizeResult stringAnalizeResult = (StringColumnAnalizeResult)analizeResult;
    if( analizeResult.getNullCount() == 0 && analizeResult.getUniqCount() == 1 ){
      return stringAnalizeResult.getUniqUtf8ByteSize();
    }
    else if( analizeResult.getNullCount() == 0 ){
      return analizeResult.getColumnSize() * Integer.BYTES + stringAnalizeResult.getTotalUtf8ByteSize();
    }
    else{
      return super.calcBinarySize( analizeResult );
    }
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    int minLength = wrapBuffer.getInt();
    char[] minCharArray = new char[minLength / Character.BYTES];
    wrapBuffer.asCharBuffer().get( minCharArray );
    wrapBuffer.position( wrapBuffer.position() + minLength );

    int maxLength = wrapBuffer.getInt();
    char[] maxCharArray = new char[maxLength / Character.BYTES];
    wrapBuffer.asCharBuffer().get( maxCharArray );
    wrapBuffer.position( wrapBuffer.position() + maxLength );

    String min = new String( minCharArray );
    String max = new String( maxCharArray );

    int type = wrapBuffer.getInt();
    int headerSize = Integer.BYTES + minLength + Integer.BYTES + maxLength + Integer.BYTES;
    if( type == 0 ){
      return new HeaderIndexLazyColumn( 
        columnBinary.columnName , 
        columnBinary.columnType , 
        new StringColumnManager( 
          columnBinary , 
          columnBinary.binaryStart + headerSize , 
          columnBinary.binaryLength - headerSize ) , 
          new RangeStringIndex( min , max , true ) 
      );
    }
    else{
      return new HeaderIndexLazyColumn(
        columnBinary.columnName ,
        columnBinary.columnType ,
        new RangeStringColumnManager(
          columnBinary ,
          columnBinary.binaryStart + headerSize ,
          columnBinary.binaryLength - headerSize ) ,
          new RangeStringIndex( min , max , false )
      );
    }
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    int minLength = wrapBuffer.getInt();
    wrapBuffer.position( wrapBuffer.position() + minLength );
    int maxLength = wrapBuffer.getInt();
    wrapBuffer.position( wrapBuffer.position() + maxLength );
    int type = wrapBuffer.getInt();
    int headerSize = Integer.BYTES + minLength + Integer.BYTES + maxLength + Integer.BYTES;

    if( type == 0 ){
      loadInMemoryStorage( columnBinary , columnBinary.binaryStart + headerSize , columnBinary.binaryLength - headerSize , allocator );
      return;
    }
    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress( columnBinary.binary , columnBinary.binaryStart + headerSize , columnBinary.binaryLength - headerSize );
    wrapBuffer = wrapBuffer.wrap( binary );
    for( int i = 0 ; i < columnBinary.rowCount ; i++ ){
      int objLength = wrapBuffer.getInt();
      allocator.setBytes( i , binary , wrapBuffer.position() , objLength );
      wrapBuffer.position( wrapBuffer.position() + objLength );
    }
    allocator.setValueCount( columnBinary.rowCount );
  }

  @Override
  public void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    int minLength = wrapBuffer.getInt();
    char[] minCharArray = new char[minLength / Character.BYTES];
    wrapBuffer.asCharBuffer().get( minCharArray );
    wrapBuffer.position( wrapBuffer.position() + minLength );

    int maxLength = wrapBuffer.getInt();
    char[] maxCharArray = new char[maxLength / Character.BYTES];
    wrapBuffer.asCharBuffer().get( maxCharArray );
    wrapBuffer.position( wrapBuffer.position() + maxLength );

    String min = new String( minCharArray );
    String max = new String( maxCharArray );

    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new StringRangeBlockIndex( min , max ) );
  }

  public class RangeStringDicManager implements IDicManager{

    private final PrimitiveObject[] dicArray;

    public RangeStringDicManager( final PrimitiveObject[] dicArray ){
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

  public class RangeStringColumnManager implements IColumnManager{

    private final ColumnBinary columnBinary;
    private final int binaryStart;
    private final int binaryLength;
    private PrimitiveColumn column;
    private boolean isCreate;

    public RangeStringColumnManager( final ColumnBinary columnBinary , final int binaryStart , final int binaryLength ) throws IOException{
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
      PrimitiveObject[] dicArray = new PrimitiveObject[ columnBinary.rowCount ];
      for( int i = 0 ; i < columnBinary.rowCount ; i++ ){
        int objLength = wrapBuffer.getInt();
        dicArray[i] = new UTF8BytesLinkObj( binary , wrapBuffer.position() , objLength );
        wrapBuffer.position( wrapBuffer.position() + objLength );
      }

      column = new PrimitiveColumn( columnBinary.columnType , columnBinary.columnName );
      IDicManager dicManager = new RangeStringDicManager( dicArray );
      column.setCellManager( new BufferDirectCellManager( columnBinary.columnType , dicManager , columnBinary.rowCount ) );

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
