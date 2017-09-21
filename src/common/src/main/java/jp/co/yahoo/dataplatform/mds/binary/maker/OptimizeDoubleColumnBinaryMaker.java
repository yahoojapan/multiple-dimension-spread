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

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;
import jp.co.yahoo.dataplatform.schema.objects.DoubleObj;

import jp.co.yahoo.dataplatform.mds.compressor.FindCompressor;
import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;
import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveCell;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.maker.index.RangeDoubleIndex;
import jp.co.yahoo.dataplatform.mds.binary.maker.index.BufferDirectSequentialNumberCellIndex;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.blockindex.DoubleRangeBlockIndex;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class OptimizeDoubleColumnBinaryMaker implements IColumnBinaryMaker{

  public static IDictionaryMaker chooseDictionaryMaker( final double min , final double max ){
    return new DoubleDictionaryMaker();
  }

  public static IDictionaryIndexMaker chooseDictionaryIndexMaker( final int dicIndexLength ){
    if( dicIndexLength < Byte.valueOf( Byte.MAX_VALUE ).intValue() ){
      return new ByteDictionaryIndexMaker();
    }
    else if( dicIndexLength < Short.valueOf( Short.MAX_VALUE ).intValue() ){
      return new ShortDictionaryIndexMaker();
    }
    else{
      return new IntDictionaryIndexMaker();
    }
  }

  public interface IDictionaryMaker{

    int getLogicalSize( final int indexLength );

    int calcBinarySize( final int dicSize );

    void create( final List<PrimitiveObject> objList , final byte[] buffer , final int start , final int length ) throws IOException;

    PrimitiveObject[] getDicPrimitiveArray( final byte[] buffer , final int start , final int length ) throws IOException;

  }

  public static class DoubleDictionaryMaker implements IDictionaryMaker{

    @Override
    public int getLogicalSize( final int indexLength ){
      return Double.BYTES * indexLength;
    }

    @Override
    public int calcBinarySize( final int dicSize ){
      return Double.BYTES * dicSize;
    }

    @Override
    public void create( final List<PrimitiveObject> objList , final byte[] buffer , final int start , final int length ) throws IOException{
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( PrimitiveObject obj : objList ){
        wrapBuffer.putDouble( obj.getDouble() );
      }
    }

    @Override                                                                                                      public PrimitiveObject[] getDicPrimitiveArray( final byte[] buffer , final int start , final int length ) throws IOException{
      int size = length / Double.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      wrapBuffer.getDouble();
      for( int i = 1 ; i < size ; i++ ){
        result[i] = new DoubleObj( wrapBuffer.getDouble() );
      }

      return result;
    }

  }

  public interface IDictionaryIndexMaker{

    int calcBinarySize( final int indexLength );

    void create( final int[] dicIndexArray , final byte[] buffer , final int start , final int length ) throws IOException;

    IntBuffer getIndexIntBuffer( final byte[] buffer , final int start , final int length ) throws IOException;

  }

  public static class ByteDictionaryIndexMaker implements IDictionaryIndexMaker{

    @Override
    public int calcBinarySize( final int indexLength ){
      return Byte.BYTES * indexLength;
    }

    @Override
    public void create( final int[] dicIndexArray , final byte[] buffer , final int start , final int length ) throws IOException{
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( int index : dicIndexArray ){
        wrapBuffer.put( (byte)index );
      }
    }

    @Override
    public IntBuffer getIndexIntBuffer( final byte[] buffer , final int start , final int length ) throws IOException{
      int size = length / Byte.BYTES;
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      IntBuffer result = IntBuffer.allocate( size );
      for( int i = 0 ; i < size ; i++ ){
        result.put( (int)( wrapBuffer.get() ) );
      }
      return result;
    }

  }

  public static class ShortDictionaryIndexMaker implements IDictionaryIndexMaker{

    @Override
    public int calcBinarySize( final int indexLength ){
      return Short.BYTES * indexLength;
    }

    @Override
    public void create( final int[] dicIndexArray , final byte[] buffer , final int start , final int length ) throws IOException{
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( int index : dicIndexArray ){
        wrapBuffer.putShort( (short)index );
      }
    }

    @Override
    public IntBuffer getIndexIntBuffer( final byte[] buffer , final int start , final int length ) throws IOException{
      int size = length / Short.BYTES;
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      IntBuffer result = IntBuffer.allocate( size );
      for( int i = 0 ; i < size ; i++ ){
        result.put( (int)( wrapBuffer.getShort() ) );
      }
      return result;
    }

  }

  public static class IntDictionaryIndexMaker implements IDictionaryIndexMaker{

    @Override
    public int calcBinarySize( final int indexLength ){
      return Integer.BYTES * indexLength;
    }

    @Override
    public void create( final int[] dicIndexArray , final byte[] buffer , final int start , final int length ) throws IOException{
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( int index : dicIndexArray ){
        wrapBuffer.putInt( index );
      }
    }

    @Override
    public IntBuffer getIndexIntBuffer( final byte[] buffer , final int start , final int length ) throws IOException{
      int size = length / Integer.BYTES;
      return ByteBuffer.wrap( buffer , start , length ).asIntBuffer();
    }

  }

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column , final MakerCache makerCache ) throws IOException{
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    Map<Double,Integer> dicMap = new HashMap<Double,Integer>();
    List<PrimitiveObject> dicList = new ArrayList<PrimitiveObject>();
    int[] indexArray = new int[column.size()];

    dicMap.put( null , Integer.valueOf(0) );
    dicList.add( new DoubleObj( (double)0 ) );

    Double min = Double.MAX_VALUE;
    Double max = Double.MIN_VALUE;
    int rowCount = 0;
    boolean hasNull = false;
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      PrimitiveObject primitiveObj = null;
      Double target = null;
      if( cell.getType() != ColumnType.NULL ){
        rowCount++;
        PrimitiveCell stringCell = (PrimitiveCell) cell;
        primitiveObj = stringCell.getRow();
        target = Double.valueOf( primitiveObj.getDouble() );
      }
      else{
        hasNull = true;
      }
      if( ! dicMap.containsKey( target ) ){
        if( 0 < min.compareTo( target ) ){
          min = Double.valueOf( target );
        }
        if( max.compareTo( target ) < 0 ){
          max = Double.valueOf( target );
        }
        dicMap.put( target , dicMap.size() );
        dicList.add( primitiveObj );
      }
      indexArray[i] = dicMap.get( target );
    }

    if( ! hasNull && min.equals( max ) ){
      return ConstantColumnBinaryMaker.createColumnBinary( new DoubleObj( min ) , column.getColumnName() , column.size() );
    }

    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker( indexArray.length );
    IDictionaryMaker dicMaker = chooseDictionaryMaker( min.doubleValue() , max.doubleValue() );

    int indexLength = indexMaker.calcBinarySize( indexArray.length );
    int dicLength = dicMaker.calcBinarySize( dicList.size() );

    byte[] binaryRaw = new byte[ indexLength + dicLength ];
    indexMaker.create( indexArray , binaryRaw , 0 , indexLength );
    dicMaker.create( dicList , binaryRaw , indexLength , dicLength );

    byte[] compressBinary = currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length );

    byte[] binary = new byte[ Double.BYTES * 2 + compressBinary.length ];

    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    wrapBuffer.putDouble( min );
    wrapBuffer.putDouble( max );
    wrapBuffer.put( compressBinary );

    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , column.getColumnType() , column.size() , binary.length , dicMaker.getLogicalSize( rowCount ) , dicList.size() , binary , 0 , binary.length , null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ){
    // TODO 後でやること
    return 0;
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Double min = Double.valueOf( wrapBuffer.getDouble() );
    Double max = Double.valueOf( wrapBuffer.getDouble() );

    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker( columnBinary.rowCount );
    IDictionaryMaker dicMaker = chooseDictionaryMaker( min.doubleValue() , max.doubleValue() );
    return new HeaderIndexLazyColumn(
      columnBinary.columnName ,
      columnBinary.columnType ,
      new ColumnManager(
        columnBinary ,
        indexMaker ,
        dicMaker
      )
      , new RangeDoubleIndex( min , max )
    );
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Double min = Double.valueOf( wrapBuffer.getDouble() );
    Double max = Double.valueOf( wrapBuffer.getDouble() );

    IDictionaryIndexMaker indexMaker = chooseDictionaryIndexMaker( columnBinary.rowCount );
    IDictionaryMaker dicMaker = chooseDictionaryMaker( min.doubleValue() , max.doubleValue() );

    int start = columnBinary.binaryStart + ( Double.BYTES * 2 );
    int length = columnBinary.binaryLength - ( Double.BYTES * 2 );

    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress( columnBinary.binary , start , length );

    int indexLength = indexMaker.calcBinarySize( columnBinary.rowCount );
    int dicLength = dicMaker.calcBinarySize( columnBinary.cardinality );

    IntBuffer indexIntBuffer = indexMaker.getIndexIntBuffer( binary , 0 , indexLength );
    PrimitiveObject[] dicArray = dicMaker.getDicPrimitiveArray( binary , indexLength , dicLength );

    int loopCount = indexIntBuffer.capacity();
    for( int i = 0 ; i < loopCount ; i++ ){
      int dicIndex = indexIntBuffer.get();
      if( dicIndex != 0 ){
        allocator.setDouble( i , dicArray[dicIndex].getDouble() );
      }
    }
    allocator.setValueCount( loopCount );
  }

  @Override
  public void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Double min = Double.valueOf( wrapBuffer.getDouble() );
    Double max = Double.valueOf( wrapBuffer.getDouble() );
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new DoubleRangeBlockIndex( min , max ) );
  }

  public class DicManager implements IDicManager{

    private final PrimitiveObject[] dicArray;

    public DicManager( final PrimitiveObject[] dicArray ) throws IOException{
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

  public class ColumnManager implements IColumnManager{

    private final ColumnBinary columnBinary;
    private final IDictionaryIndexMaker indexMaker;
    private final IDictionaryMaker dicMaker;

    private PrimitiveColumn column;
    private boolean isCreate;

    public ColumnManager( final ColumnBinary columnBinary , final IDictionaryIndexMaker indexMaker , final IDictionaryMaker dicMaker ){
      this.columnBinary = columnBinary;
      this.indexMaker = indexMaker;
      this.dicMaker = dicMaker;
    }

    public void create() throws IOException{
      if( isCreate ){
        return;
      }
      int start = columnBinary.binaryStart + ( Double.BYTES * 2 );
      int length = columnBinary.binaryLength - ( Double.BYTES * 2 );

      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] binary = compressor.decompress( columnBinary.binary , start , length );

      int indexLength = indexMaker.calcBinarySize( columnBinary.rowCount );
      int dicLength = dicMaker.calcBinarySize( columnBinary.cardinality );

      IntBuffer indexIntBuffer = indexMaker.getIndexIntBuffer( binary , 0 , indexLength );
      PrimitiveObject[] dicArray = dicMaker.getDicPrimitiveArray( binary , indexLength , dicLength );

      IDicManager dicManager = new DicManager( dicArray );
      column = new PrimitiveColumn( columnBinary.columnType , columnBinary.columnName );
      column.setCellManager( new BufferDirectDictionaryLinkCellManager( columnBinary.columnType , dicManager , indexIntBuffer ) );
      column.setIndex( new BufferDirectSequentialNumberCellIndex( columnBinary.columnType , dicManager , indexIntBuffer ) );

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
