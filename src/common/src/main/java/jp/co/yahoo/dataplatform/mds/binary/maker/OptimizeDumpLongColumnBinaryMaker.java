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

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;
import jp.co.yahoo.dataplatform.schema.objects.ByteObj;
import jp.co.yahoo.dataplatform.schema.objects.ShortObj;
import jp.co.yahoo.dataplatform.schema.objects.IntegerObj;
import jp.co.yahoo.dataplatform.schema.objects.LongObj;

import jp.co.yahoo.dataplatform.mds.compressor.FindCompressor;
import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;
import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveCell;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.ByteColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.ShortColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.IntegerColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.LongColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.maker.index.RangeLongIndex;
import jp.co.yahoo.dataplatform.mds.binary.maker.index.SequentialNumberCellIndex;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.blockindex.LongRangeBlockIndex;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class OptimizeDumpLongColumnBinaryMaker implements IColumnBinaryMaker{

  public static ColumnType getDiffColumnType( final long min , final long max ){
    long diff = max - min;
    if( diff < 0 ){
      return ColumnType.LONG;
    }

    if( diff <= Byte.MAX_VALUE ){
      return ColumnType.BYTE;
    }
    else if( diff <= Short.MAX_VALUE ){
      return ColumnType.SHORT;
    }
    else if( diff <= Integer.MAX_VALUE ){
      return ColumnType.INTEGER;
    }

    return ColumnType.LONG;
  }

  public static PrimitiveObject createConstObject( final ColumnType type , final long num ) throws IOException{
    switch( type ){
      case BYTE:
        return new ByteObj( Long.valueOf( num ).byteValue() );
      case SHORT:
        return new ShortObj( Long.valueOf( num ).shortValue() );
      case INTEGER:
        return new IntegerObj( Long.valueOf( num ).intValue() );
      default:
        return new LongObj( num );
    }
  }

  public static IBinaryMaker chooseBinaryMaker( final long min , final long max ){
    ColumnType diffType = getDiffColumnType( min , max );

    if( Byte.valueOf( Byte.MIN_VALUE ).longValue() <= min && max <= Byte.valueOf( Byte.MAX_VALUE ).longValue() ){
      return new ByteBinaryMaker();
    }
    else if( diffType == ColumnType.BYTE ){
      return new DiffByteBinaryMaker( min );
    }
    else if( Short.valueOf( Short.MIN_VALUE ).longValue() <= min && max <= Short.valueOf( Short.MAX_VALUE ).longValue() ){
      return new ShortBinaryMaker();
    }
    else if( diffType == ColumnType.SHORT ){
      return new DiffShortBinaryMaker( min );
    }
    else if( Integer.valueOf( Integer.MIN_VALUE ).longValue() <= min && max <= Integer.valueOf( Integer.MAX_VALUE ).longValue() ){
      return new IntBinaryMaker();
    }
    else if( diffType == ColumnType.INTEGER ){
      return new DiffIntBinaryMaker( min );
    }
    else{
      return new LongBinaryMaker();
    }
  }

  public interface IBinaryMaker{

    int getLogicalSize( final int columnSize );

    int calcBinarySize( final int columnSize );

    void create( final long[] longArray , final byte[] buffer , final int start , final int length ) throws IOException;

    PrimitiveObject[] getPrimitiveArray( final byte[] buffer , final int start , final int length ) throws IOException;

    void loadInMemoryStorage( final byte[] buffer , final int start , final int length , final IMemoryAllocator allocator , final byte[] isNullArray , final int size ) throws IOException;

  }

  public static class ByteBinaryMaker implements IBinaryMaker{

    @Override
    public int getLogicalSize( final int columnSize ){
      return Byte.BYTES * columnSize;
    }

    @Override
    public int calcBinarySize( final int columnSize ){
      return Byte.BYTES * columnSize;
    }

    @Override
    public void create( final long[] longArray , final byte[] buffer , final int start , final int length ) throws IOException{
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( long value : longArray ){
        wrapBuffer.put( Long.valueOf( value ).byteValue() );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray( final byte[] buffer , final int start , final int length ) throws IOException{
      int size = length / Byte.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( int i = 0 ; i < size ; i++ ){
        result[i] = new ByteObj( wrapBuffer.get() );
      }
      return result;
    }

    @Override
    public void loadInMemoryStorage( final byte[] buffer , final int start , final int length , final IMemoryAllocator allocator , final byte[] isNullArray , final int size ) throws IOException{
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( int i = 0 ; i < size ; i++ ){
        if( isNullArray[i] == 0 ){
          allocator.setByte( i , wrapBuffer.get() );
        }
        else{
          wrapBuffer.position( wrapBuffer.position() + Byte.BYTES );
          allocator.setNull( i );
        }
      }
    }

  }

  public static class DiffByteBinaryMaker implements IBinaryMaker{

    private final long min;

    public DiffByteBinaryMaker( final long min ){
      this.min = min;
    }

    @Override
    public int getLogicalSize( final int columnSize ){
      return Byte.BYTES * columnSize;
    }

    @Override
    public int calcBinarySize( final int columnSize ){
      return Byte.BYTES * columnSize;
    }

    @Override
    public void create( final long[] longArray , final byte[] buffer , final int start , final int length ) throws IOException{
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( long value : longArray ){
        byte castValue = (byte)( value - min );
        wrapBuffer.put( castValue );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray( final byte[] buffer , final int start , final int length ) throws IOException{
      int size = length / Byte.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( int i = 0 ; i < size ; i++ ){
        result[i] = new LongObj( (long)( wrapBuffer.get() ) + min );
      }
      return result;
    }

    @Override
    public void loadInMemoryStorage( final byte[] buffer , final int start , final int length , final IMemoryAllocator allocator , final byte[] isNullArray , final int size ) throws IOException{
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( int i = 0 ; i < size ; i++ ){
        if( isNullArray[i] == 0 ){
          allocator.setLong( i , (long)( wrapBuffer.get() ) + min );
        }
        else{
          wrapBuffer.position( wrapBuffer.position() + Byte.BYTES );
          allocator.setNull( i );
        }
      }
    }

  }

  public static class ShortBinaryMaker implements IBinaryMaker{

    @Override
    public int getLogicalSize( final int columnSize ){
      return Short.BYTES * columnSize;
    }

    @Override
    public int calcBinarySize( final int columnSize ){
      return Short.BYTES * columnSize;
    }

    @Override
    public void create( final long[] longArray , final byte[] buffer , final int start , final int length ) throws IOException{
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( long value : longArray ){
        wrapBuffer.putShort( Long.valueOf( value ).shortValue() );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray( final byte[] buffer , final int start , final int length ) throws IOException{
      int size = length / Short.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( int i = 0 ; i < size ; i++ ){
        result[i] = new ShortObj( wrapBuffer.getShort() );
      }
      return result;
    }

    @Override
    public void loadInMemoryStorage( final byte[] buffer , final int start , final int length , final IMemoryAllocator allocator , final byte[] isNullArray , final int size ) throws IOException{
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( int i = 0 ; i < size ; i++ ){
        if( isNullArray[i] == 0){
          allocator.setShort( i , wrapBuffer.getShort() );
        }
        else{
          wrapBuffer.position( wrapBuffer.position() + Short.BYTES );
          allocator.setNull( i );
        }
      }
    }

  }

  public static class DiffShortBinaryMaker implements IBinaryMaker{

    private final long min;

    public DiffShortBinaryMaker( final long min ){
      this.min = min;
    }

    @Override
    public int getLogicalSize( final int columnSize ){
      return Short.BYTES * columnSize;
    }

    @Override
    public int calcBinarySize( final int columnSize ){
      return Short.BYTES * columnSize;
    }

    @Override
    public void create( final long[] longArray , final byte[] buffer , final int start , final int length ) throws IOException{
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( long value : longArray ){
        short castValue = (short)( value - min );
        wrapBuffer.putShort( castValue );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray( final byte[] buffer , final int start , final int length ) throws IOException{
      int size = length / Short.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( int i = 0 ; i < size ; i++ ){
        result[i] = new LongObj( (long)( wrapBuffer.getShort() ) + min );
      }
      return result;
    }

    @Override
    public void loadInMemoryStorage( final byte[] buffer , final int start , final int length , final IMemoryAllocator allocator , final byte[] isNullArray , final int size ) throws IOException{
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( int i = 0 ; i < size ; i++ ){
        if( isNullArray[i] == 0 ){
          allocator.setLong( i , (long)( wrapBuffer.getShort() ) + min );
        }
        else{
          wrapBuffer.position( wrapBuffer.position() + Short.BYTES );
          allocator.setNull( i );
        }
      }
    }

  }

  public static class IntBinaryMaker implements IBinaryMaker{

    @Override
    public int getLogicalSize( final int columnSize ){
      return Integer.BYTES * columnSize;
    }

    @Override
    public int calcBinarySize( final int columnSize ){
      return Integer.BYTES * columnSize;
    }

    @Override
    public void create( final long[] longArray , final byte[] buffer , final int start , final int length ) throws IOException{
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( long value : longArray ){
        wrapBuffer.putInt( Long.valueOf( value ).intValue() );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray( final byte[] buffer , final int start , final int length ) throws IOException{
      int size = length / Integer.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( int i = 0 ; i < size ; i++ ){
        result[i] = new IntegerObj( wrapBuffer.getInt() );
      }
      return result;
    }

    @Override
    public void loadInMemoryStorage( final byte[] buffer , final int start , final int length , final IMemoryAllocator allocator , final byte[] isNullArray , final int size ) throws IOException{
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( int i = 0 ; i < size ; i++ ){
        if( isNullArray[i] == 0 ){
          allocator.setInteger( i , wrapBuffer.getInt() );
        }
        else{
          wrapBuffer.position( wrapBuffer.position() + Integer.BYTES );
          allocator.setNull( i );
        }
      }
    }

  }

  public static class DiffIntBinaryMaker implements IBinaryMaker{

    private final long min;

    public DiffIntBinaryMaker( final long min ){
      this.min = min;
    }

    @Override
    public int getLogicalSize( final int columnSize ){
      return Long.BYTES * columnSize;
    }

    @Override
    public int calcBinarySize( final int columnSize ){
      return Integer.BYTES * columnSize;
    }

    @Override
    public void create( final long[] longArray , final byte[] buffer , final int start , final int length ) throws IOException{
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( long value : longArray ){
        int castValue = (int)( value - min );
        wrapBuffer.putInt( castValue );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray( final byte[] buffer , final int start , final int length ) throws IOException{
      int size = length / Integer.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( int i = 0 ; i < size ; i++ ){
        result[i] = new LongObj( (long)( wrapBuffer.getInt() ) + min );
      }
      return result;
    }

    public void loadInMemoryStorage( final byte[] buffer , final int start , final int length , final IMemoryAllocator allocator , final byte[] isNullArray , final int size ) throws IOException{
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( int i = 0 ; i < size ; i++ ){
        if( isNullArray[i] == 0 ){
          allocator.setLong( i , (long)( wrapBuffer.getInt() ) + min );
        }
        else{
          wrapBuffer.position( wrapBuffer.position() + Integer.BYTES );
          allocator.setNull( i );
        }
      }
    }

  }

  public static class LongBinaryMaker implements IBinaryMaker{

    @Override
    public int getLogicalSize( final int columnSize ){
      return Long.BYTES * columnSize;
    }

    @Override
    public int calcBinarySize( final int columnSize ){
      return Long.BYTES * columnSize;
    }

    @Override
    public void create( final long[] longArray , final byte[] buffer , final int start , final int length ) throws IOException{
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( long value : longArray ){
        wrapBuffer.putLong( value );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveArray( final byte[] buffer , final int start , final int length ) throws IOException{
      int size = length / Long.BYTES;
      PrimitiveObject[] result = new PrimitiveObject[size];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( int i = 0 ; i < size ; i++ ){
        result[i] = new LongObj( wrapBuffer.getLong() );
      }
      return result;
    }

    @Override
    public void loadInMemoryStorage( final byte[] buffer , final int start , final int length , final IMemoryAllocator allocator , final byte[] isNullArray , final int size ) throws IOException{
      ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
      for( int i = 0 ; i < size ; i++ ){
        if( isNullArray[i] == 0 ){
          allocator.setLong( i , wrapBuffer.getLong() );
        }
        else{
          wrapBuffer.position( wrapBuffer.position() + Long.BYTES );
          allocator.setNull( i );
        }
      }
    }

  }

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column ) throws IOException{
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    long[] valueArray = new long[column.size()];
    byte[] isNullArray = new byte[column.size()];

    Long min = Long.MAX_VALUE;
    Long max = Long.MIN_VALUE;
    int rowCount = 0;
    boolean hasNull = false;
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      PrimitiveObject primitiveObj = null;
      if( cell.getType() == ColumnType.NULL ){
        hasNull = true;
        isNullArray[i] = 1;
      }
      else{
        rowCount++;
        PrimitiveCell stringCell = (PrimitiveCell) cell;
        primitiveObj = stringCell.getRow();
        valueArray[i] = primitiveObj.getLong();
        if( 0 < min.compareTo( valueArray[i] ) ){
          min = Long.valueOf( valueArray[i] );
        }
        if( max.compareTo( valueArray[i] ) < 0 ){
          max = Long.valueOf( valueArray[i] );
        }
      }
    }

    if( ! hasNull && min.equals( max ) ){
      return ConstantColumnBinaryMaker.createColumnBinary( createConstObject( column.getColumnType() , min ) , column.getColumnName() , column.size() );
    }

    IBinaryMaker binaryMaker = chooseBinaryMaker( min.longValue() , max.longValue() );

    int nullBinaryLength = isNullArray.length;
    int valueLength = binaryMaker.calcBinarySize( valueArray.length );

    byte[] binaryRaw = new byte[ nullBinaryLength + valueLength ];
    ByteBuffer compressBinaryBuffer = ByteBuffer.wrap( binaryRaw , 0 , nullBinaryLength );
    compressBinaryBuffer.put( isNullArray );
    binaryMaker.create( valueArray , binaryRaw , nullBinaryLength , valueLength );

    byte[] compressBinary = currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length );

    byte[] binary = new byte[ Long.BYTES * 2 + compressBinary.length ];

    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    wrapBuffer.putLong( min );
    wrapBuffer.putLong( max );
    wrapBuffer.put( compressBinary );

    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , column.getColumnType() , column.size() , binary.length , binaryMaker.getLogicalSize( rowCount ) , -1 , binary , 0 , binary.length , null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ){
    long min;
    long max;
    switch( analizeResult.getColumnType() ){
      case BYTE:
        min = (long)( (ByteColumnAnalizeResult) analizeResult ).getMin();
        max = (long)( (ByteColumnAnalizeResult) analizeResult ).getMax();
        break;
      case SHORT:
        min = (long)( (ShortColumnAnalizeResult) analizeResult ).getMin();
        max = (long)( (ShortColumnAnalizeResult) analizeResult ).getMax();
        break;
      case INTEGER:
        min = (long)( (IntegerColumnAnalizeResult) analizeResult ).getMin();
        max = (long)( (IntegerColumnAnalizeResult) analizeResult ).getMax();
        break;
      case LONG:
        min = ( (LongColumnAnalizeResult) analizeResult ).getMin();
        max = ( (LongColumnAnalizeResult) analizeResult ).getMax();
        break;
      default:
        min = Long.MIN_VALUE;
        max = Long.MAX_VALUE;
        break;
    }
    IBinaryMaker binaryMaker = chooseBinaryMaker( min , max );
    int nullBinaryLength = analizeResult.getColumnSize();
    int valueLength = binaryMaker.calcBinarySize( analizeResult.getColumnSize() );
    return nullBinaryLength + valueLength;
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Long min = Long.valueOf( wrapBuffer.getLong() );
    Long max = Long.valueOf( wrapBuffer.getLong() );

    IBinaryMaker binaryMaker = chooseBinaryMaker( min.longValue() , max.longValue() );
    return new HeaderIndexLazyColumn(
      columnBinary.columnName ,
      columnBinary.columnType ,
      new ColumnManager(
        columnBinary ,
        binaryMaker
      )
      , new RangeLongIndex( min , max )
    );
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Long min = Long.valueOf( wrapBuffer.getLong() );
    Long max = Long.valueOf( wrapBuffer.getLong() );

    IBinaryMaker binaryMaker = chooseBinaryMaker( min.longValue() , max.longValue() );

    int start = columnBinary.binaryStart + ( Long.BYTES * 2 );
    int length = columnBinary.binaryLength - ( Long.BYTES * 2 );

    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress( columnBinary.binary , start , length );

    int isNullLength = columnBinary.rowCount;
    int binaryLength = binaryMaker.calcBinarySize( columnBinary.rowCount );

    binaryMaker.loadInMemoryStorage( binary , isNullLength , binaryLength , allocator , binary , columnBinary.rowCount );

    allocator.setValueCount( columnBinary.rowCount );
  }

  @Override
  public void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary , final int spreadIndex ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Long min = Long.valueOf( wrapBuffer.getLong() );
    Long max = Long.valueOf( wrapBuffer.getLong() );
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new LongRangeBlockIndex( min , max ) );
  }

  public class DicManager implements IDicManager{

    private final PrimitiveObject[] dicArray;
    private final byte[] nullArray;

    public DicManager( final PrimitiveObject[] dicArray , final byte[] nullArray ) throws IOException{
      this.dicArray = dicArray;
      this.nullArray = nullArray;
    }

    @Override
    public PrimitiveObject get( final int index ) throws IOException{
      if( nullArray[index] == 0 ){
        return dicArray[index];
      }
      else{
        return null;
      }
    }

    @Override
    public int getDicSize() throws IOException{
      return dicArray.length;
    }

  }

  public class ColumnManager implements IColumnManager{

    private final ColumnBinary columnBinary;
    private final IBinaryMaker binaryMaker;

    private PrimitiveColumn column;
    private boolean isCreate;

    public ColumnManager( final ColumnBinary columnBinary , final IBinaryMaker binaryMaker ){
      this.columnBinary = columnBinary;
      this.binaryMaker = binaryMaker;
    }

    public void create() throws IOException{
      if( isCreate ){
        return;
      }
      int start = columnBinary.binaryStart + ( Long.BYTES * 2 );
      int length = columnBinary.binaryLength - ( Long.BYTES * 2 );

      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] binary = compressor.decompress( columnBinary.binary , start , length );

      int isNullLength = columnBinary.rowCount;
      int binaryLength = binaryMaker.calcBinarySize( columnBinary.rowCount );

      PrimitiveObject[] dicArray = binaryMaker.getPrimitiveArray( binary , isNullLength , binaryLength );

      IDicManager dicManager = new DicManager( dicArray , binary );
      column = new PrimitiveColumn( columnBinary.columnType , columnBinary.columnName );
      column.setCellManager( new BufferDirectCellManager( columnBinary.columnType , dicManager , columnBinary.rowCount ) );
      column.setIndex( new SequentialNumberCellIndex( columnBinary.columnType , dicManager ) );

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
