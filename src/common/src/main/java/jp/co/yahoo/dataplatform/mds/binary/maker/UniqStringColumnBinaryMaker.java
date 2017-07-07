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
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;
import jp.co.yahoo.dataplatform.schema.objects.StringObj;

import jp.co.yahoo.dataplatform.mds.binary.BinaryDump;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.maker.cache.ByteBufferCache;
import jp.co.yahoo.dataplatform.mds.binary.maker.index.BufferDirectSequentialStringCellIndex;
import jp.co.yahoo.dataplatform.mds.compressor.FindCompressor;
import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;
import jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength;
import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveCell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class UniqStringColumnBinaryMaker implements IColumnBinaryMaker{

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column , final MakerCache makerCache ) throws IOException{
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    Map<String,Integer> dicMap = new HashMap<String,Integer>();
    List<Integer> columnIndexList = new ArrayList<Integer>();
    List<String> stringList = new ArrayList<String>();

    dicMap.put( null , Integer.valueOf(0) );
    stringList.add( new String() );

    int totalLength = 0;
    int logicalTotalLength = 0;
    int rowCount = 0;
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      String targetStr = null;
      int strLength = 0;
      if( cell.getType() != ColumnType.NULL ){
        rowCount++;
        PrimitiveCell stringCell = (PrimitiveCell) cell;
        targetStr = stringCell.getRow().getString();
        if( targetStr != null ){
          strLength = targetStr.length() * PrimitiveByteLength.CHAR_LENGTH;
          logicalTotalLength += strLength;
        }
      }
      if( ! dicMap.containsKey( targetStr ) ){
        dicMap.put( targetStr , stringList.size() );
        stringList.add( targetStr );
        totalLength += strLength;
      }
      columnIndexList.add( dicMap.get( targetStr ) );
    }

    int rawSize = ( PrimitiveByteLength.INT_LENGTH * columnIndexList.size() ) + ( PrimitiveByteLength.INT_LENGTH + totalLength + ( PrimitiveByteLength.INT_LENGTH * stringList.size() ) ) + ( PrimitiveByteLength.INT_LENGTH * 2 );
    ICache toBinaryCache = makerCache.getCache( "to_binary_raw_cache" );
    if( ! ( toBinaryCache instanceof ByteBufferCache) ){
      toBinaryCache = new ByteBufferCache();
      makerCache.registerCache( "to_binary_raw_cache" , toBinaryCache );
    }
    ByteBuffer rawByteBuffer = ( (ByteBufferCache)toBinaryCache ).get();
    if( rawByteBuffer == null || rawByteBuffer.capacity() < rawSize ){
      rawByteBuffer = ByteBuffer.allocate( rawSize );
      toBinaryCache.register( rawByteBuffer );
    }
    rawByteBuffer.position(0);
    rawByteBuffer.putInt( PrimitiveByteLength.INT_LENGTH * columnIndexList.size() );
    BinaryDump.appendIntegerToByteBuffer( columnIndexList , rawByteBuffer );
    rawByteBuffer.putInt( PrimitiveByteLength.INT_LENGTH + totalLength + ( PrimitiveByteLength.INT_LENGTH * stringList.size() ) );
    BinaryDump.appendStringToByteBuffer( stringList , totalLength , rawByteBuffer );

    int rawLength = rawByteBuffer.position();
    byte[] binaryRaw = rawByteBuffer.array();

    byte[] binary = currentConfig.compressorClass.compress( binaryRaw , 0 , rawLength );

    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.STRING , rowCount , binaryRaw.length , logicalTotalLength , dicMap.size() , binary , 0 , binary.length , null );
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
    return new LazyColumn( columnBinary.columnName , columnBinary.columnType , new StringColumnManager( columnBinary , primitiveObjectConnector ) );
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    int decompressSize = compressor.getDecompressSize( columnBinary.binary , columnBinary.binaryStart , columnBinary.
binaryLength );
    byte[] decompressBuffer = new byte[decompressSize];

    int binaryLength = compressor.decompressAndSet( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength , decompressBuffer );

    byte[] binary = decompressBuffer;
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binaryLength );
    int offset = 0;

    int columnIndexBinaryLength = wrapBuffer.getInt( offset );
    offset += PrimitiveByteLength.INT_LENGTH;
    int columnIndexBinaryStart = offset;
    offset += columnIndexBinaryLength;

    int dicBinaryLength = wrapBuffer.getInt( offset );
    offset += PrimitiveByteLength.INT_LENGTH;
    int dicBinaryStart = offset;
    offset += dicBinaryLength;

    IntBuffer indexIntBuffer = BinaryDump.binaryToIntBuffer( decompressBuffer , columnIndexBinaryStart , columnIndexBinaryLength );
    List<String> dicArray = BinaryDump.binaryToStringList( binary , dicBinaryStart , dicBinaryLength );
    int size = indexIntBuffer.capacity();
    for( int i = 0 ; i < size ; i++ ){
      int dicIndex = indexIntBuffer.get();
      if( dicIndex != 0 ){
        allocator.setString( i , dicArray.get( dicIndex ) );
      }
    }
    allocator.setValueCount( size );
  }

  public class StringDicManager implements IDicManager{

    private final List<PrimitiveObject> dicList;
    private final int dicSize;

    public StringDicManager( final List<PrimitiveObject> dicList ) throws IOException{
      this.dicList = dicList;
      dicSize = dicList.size();
    }

    @Override
    public PrimitiveObject get( final int index ) throws IOException{
      return dicList.get( index );
    }

    @Override
    public int getDicSize() throws IOException{
      return dicSize;
    }
  }

  public class StringColumnManager implements IColumnManager{

    private final IPrimitiveObjectConnector primitiveObjectConnector;
    private final ColumnBinary columnBinary;
    private PrimitiveColumn column;
    private boolean isCreate;

    public StringColumnManager( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
      this.columnBinary = columnBinary;
      this.primitiveObjectConnector = primitiveObjectConnector;
    }

    private void create() throws IOException{
      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      int decompressSize = compressor.getDecompressSize( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
      byte[] decompressBuffer = new byte[decompressSize];

      List<PrimitiveObject> dicList = new ArrayList<PrimitiveObject>();

      int binaryLength = compressor.decompressAndSet( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength , decompressBuffer );
      byte[] binary = decompressBuffer;
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binaryLength );
      int offset = 0;

      int columnIndexBinaryLength = wrapBuffer.getInt( offset );
      offset += PrimitiveByteLength.INT_LENGTH;
      int columnIndexBinaryStart = offset;
      offset += columnIndexBinaryLength;

      int dicBinaryLength = wrapBuffer.getInt( offset );
      offset += PrimitiveByteLength.INT_LENGTH;
      int dicBinaryStart = offset;
      offset += dicBinaryLength;

      IntBuffer indexIntBuffer = BinaryDump.binaryToIntBuffer( decompressBuffer , columnIndexBinaryStart , columnIndexBinaryLength );

      List<String> readString = BinaryDump.binaryToStringList( binary , dicBinaryStart , dicBinaryLength );
      for( String str : readString ){
        dicList.add( primitiveObjectConnector.convert( PrimitiveType.STRING , new StringObj( str ) ) );
      }
      IDicManager dicManager = new StringDicManager( dicList );

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
