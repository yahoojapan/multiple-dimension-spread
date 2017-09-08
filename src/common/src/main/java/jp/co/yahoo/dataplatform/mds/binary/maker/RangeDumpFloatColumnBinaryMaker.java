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
import java.nio.FloatBuffer;

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
import jp.co.yahoo.dataplatform.mds.spread.analyzer.IColumnAnalizeResult;

import jp.co.yahoo.dataplatform.schema.objects.FloatObj;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveType;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.maker.index.RangeFloatIndex;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.blockindex.FloatRangeBlockIndex;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class RangeDumpFloatColumnBinaryMaker extends DumpFloatColumnBinaryMaker{

  private static final int HEADER_SIZE = ( PrimitiveByteLength.FLOAT_LENGTH * 2 ) + PrimitiveByteLength.INT_LENGTH;

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column , final MakerCache makerCache ) throws IOException{
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    byte[] parentsBinaryRaw = new byte[ ( PrimitiveByteLength.INT_LENGTH * 2 ) + column.size() + ( column.size() * PrimitiveByteLength.FLOAT_LENGTH ) ];
    ByteBuffer lengthBuffer = ByteBuffer.wrap( parentsBinaryRaw );
    lengthBuffer.putInt( column.size() );
    lengthBuffer.putInt( column.size() * PrimitiveByteLength.FLOAT_LENGTH );

    ByteBuffer nullFlagBuffer = ByteBuffer.wrap( parentsBinaryRaw , PrimitiveByteLength.INT_LENGTH * 2 , column.size() );
    FloatBuffer floatBuffer = ByteBuffer.wrap( parentsBinaryRaw , ( PrimitiveByteLength.INT_LENGTH * 2 + column.size() ) , ( column.size() * PrimitiveByteLength.FLOAT_LENGTH ) ).asFloatBuffer();
    int rowCount = 0;
    boolean hasNull = false;
    Float min = Float.MAX_VALUE;
    Float max = Float.MIN_VALUE;
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      if( cell.getType() == ColumnType.NULL ){
        nullFlagBuffer.put( (byte)1 );
        floatBuffer.put( (float)0 );
        hasNull = true;
      }
      else{
        rowCount++;
        PrimitiveCell byteCell = (PrimitiveCell) cell;
        nullFlagBuffer.put( (byte)0 );
        Float target = Float.valueOf( byteCell.getRow().getFloat() );
        floatBuffer.put( target );
        if( 0 < min.compareTo( target ) ){
          min = Float.valueOf( target );
        }
        if( max.compareTo( target ) < 0 ){
          max = Float.valueOf( target );
        }
      }
    }

    byte[] binary;
    int rawLength;
    if( hasNull ){
      byte[] binaryRaw = parentsBinaryRaw;
      byte[] compressBinaryRaw = currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length );
      rawLength = binaryRaw.length;
      
      binary = new byte[ HEADER_SIZE + compressBinaryRaw.length ];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary );
      wrapBuffer.putFloat( min );
      wrapBuffer.putFloat( max );
      wrapBuffer.putInt( 0 );
      wrapBuffer.put( compressBinaryRaw );
    }
    else{
      rawLength = column.size() * PrimitiveByteLength.FLOAT_LENGTH;
      byte[] compressBinaryRaw = currentConfig.compressorClass.compress( parentsBinaryRaw , ( PrimitiveByteLength.INT_LENGTH * 2 ) + column.size() , column.size() * PrimitiveByteLength.FLOAT_LENGTH );

      binary = new byte[ HEADER_SIZE + compressBinaryRaw.length ];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary );
      wrapBuffer.putFloat( min );
      wrapBuffer.putFloat( max );
      wrapBuffer.putInt( 1 );
      wrapBuffer.put( compressBinaryRaw );
    }
    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.FLOAT , rowCount , rawLength , rowCount * PrimitiveByteLength.FLOAT_LENGTH , -1 , binary , 0 , binary.length , null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ){
    if( analizeResult.getNullCount() == 0 ){
      return analizeResult.getColumnSize() * PrimitiveByteLength.FLOAT_LENGTH;
    }
    else{
      return super.calcBinarySize( analizeResult );
    }
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Float min = Float.valueOf( wrapBuffer.getFloat() );
    Float max = Float.valueOf( wrapBuffer.getFloat() );
    int type = wrapBuffer.getInt();
    if( type == 0 ){
      return new HeaderIndexLazyColumn( 
        columnBinary.columnName , 
        columnBinary.columnType , 
        new FloatColumnManager( 
          columnBinary , 
          primitiveObjectConnector , 
          columnBinary.binaryStart + HEADER_SIZE , 
          columnBinary.binaryLength - HEADER_SIZE ) , 
          new RangeFloatIndex( min , max ) 
      );
    }
    else{
      return new HeaderIndexLazyColumn(
        columnBinary.columnName ,
        columnBinary.columnType ,
        new RangeFloatColumnManager(
          columnBinary ,
          primitiveObjectConnector ,
          columnBinary.binaryStart + HEADER_SIZE ,
          columnBinary.binaryLength - HEADER_SIZE ) ,
          new RangeFloatIndex( min , max )
      );
    }
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    wrapBuffer.position( PrimitiveByteLength.FLOAT_LENGTH * 2 );
    int type = wrapBuffer.getInt();
    if( type == 0 ){
      loadInMemoryStorage( columnBinary , columnBinary.binaryStart + HEADER_SIZE , columnBinary.binaryLength - HEADER_SIZE , allocator );
      return;
    }
    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress( columnBinary.binary , columnBinary.binaryStart + HEADER_SIZE , columnBinary.binaryLength - HEADER_SIZE );
    wrapBuffer = wrapBuffer.wrap( binary );
    for( int i = 0 ; i < columnBinary.rowCount ; i++ ){
      allocator.setFloat( i , wrapBuffer.getFloat() );
    }
    allocator.setValueCount( columnBinary.rowCount );
  }

  @Override
  public void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Float min = Float.valueOf( wrapBuffer.getFloat() );
    Float max = Float.valueOf( wrapBuffer.getFloat() );
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new FloatRangeBlockIndex( min , max ) );
  }

  public class RangeFloatDicManager implements IDicManager{

    private final IPrimitiveObjectConnector primitiveObjectConnector;
    private final FloatBuffer dicBuffer;
    private final int dicLength;

    public RangeFloatDicManager( final IPrimitiveObjectConnector primitiveObjectConnector , final FloatBuffer dicBuffer ){
      this.primitiveObjectConnector = primitiveObjectConnector;
      this.dicBuffer = dicBuffer;
      dicLength = dicBuffer.capacity(); 
    }

    @Override
    public PrimitiveObject get( final int index ) throws IOException{
      return primitiveObjectConnector.convert( PrimitiveType.FLOAT , new FloatObj( dicBuffer.get( index ) ) );
    }

    @Override
    public int getDicSize() throws IOException{
      return dicLength;
    }

  }

  public class RangeFloatColumnManager implements IColumnManager{

    private final IPrimitiveObjectConnector primitiveObjectConnector;
    private final ColumnBinary columnBinary;
    private final int binaryStart;
    private final int binaryLength;
    private PrimitiveColumn column;
    private boolean isCreate;

    public RangeFloatColumnManager( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector , final int binaryStart , final int binaryLength ) throws IOException{
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

      column = new PrimitiveColumn( columnBinary.columnType , columnBinary.columnName );
      IDicManager dicManager = new RangeFloatDicManager( primitiveObjectConnector , ByteBuffer.wrap( binary ).asFloatBuffer() );
      column.setCellManager( new BufferDirectCellManager( ColumnType.INTEGER , dicManager , columnBinary.rowCount ) );

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
