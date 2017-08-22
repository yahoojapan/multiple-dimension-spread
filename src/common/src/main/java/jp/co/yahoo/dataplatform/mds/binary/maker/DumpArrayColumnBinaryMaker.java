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

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.ArrayCell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.column.ArrayColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.SpreadArrayLink;
import jp.co.yahoo.dataplatform.mds.spread.column.ICellManager;
import jp.co.yahoo.dataplatform.mds.spread.column.index.ICellIndex;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.IFilter;
import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;
import jp.co.yahoo.dataplatform.mds.compressor.FindCompressor;
import jp.co.yahoo.dataplatform.mds.binary.BinaryDump;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.FindColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionIndex;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class DumpArrayColumnBinaryMaker implements IColumnBinaryMaker{

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column , final MakerCache makerCache ) throws IOException{
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }

    List<Integer> numberList = new ArrayList<Integer>();
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      if( cell instanceof ArrayCell ){
        ArrayCell arrayCell = (ArrayCell) cell;
        numberList.add( arrayCell.getEnd() - arrayCell.getStart() );
      }
      else{
        numberList.add( 0 );
      }
    }

    byte[] binary = BinaryDump.dumpInteger( numberList );

    byte[] compressData = currentConfig.compressorClass.compress( binary , 0 , binary.length );

    IColumn childColumn = column.getColumn( 0 );
    List<ColumnBinary> columnBinaryList = new ArrayList<ColumnBinary>();

    ColumnBinaryMakerCustomConfigNode childColumnConfigNode = null;
    IColumnBinaryMaker maker = commonConfig.getColumnMaker( childColumn.getColumnType() );
    if( currentConfigNode != null ){
      childColumnConfigNode = currentConfigNode.getChildConfigNode( childColumn.getColumnName() );
      if( childColumnConfigNode != null ){
        maker = childColumnConfigNode.getCurrentConfig().getColumnMaker( childColumn.getColumnType() );
      }
    }
    columnBinaryList.add( maker.toBinary( commonConfig , childColumnConfigNode , childColumn , makerCache.getChild( childColumn.getColumnName() ) ) );
    
    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.ARRAY , column.size() , binary.length , 0 , -1 , compressData , 0 , compressData.length , columnBinaryList );
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
    return new LazyColumn( columnBinary.columnName , columnBinary.columnType , new ArrayColumnManager( columnBinary , primitiveObjectConnector ) );
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    for( ColumnBinary childColumnBinary : columnBinary.columnBinaryList ){
      IColumnBinaryMaker maker = FindColumnBinaryMaker.get( childColumnBinary.makerClassName );
      IMemoryAllocator childMemoryAllocator = allocator.getChild( childColumnBinary.columnName , childColumnBinary.columnType );
      maker.loadInMemoryStorage( childColumnBinary , childMemoryAllocator );
    }

    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    int decompressSize = compressor.getDecompressSize( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    byte[] decompressBuffer = new byte[decompressSize];
    int binaryLength = compressor.decompressAndSet( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength , decompressBuffer );

    IntBuffer buffer = ByteBuffer.wrap( decompressBuffer , 0 , binaryLength ).asIntBuffer();
    int length = buffer.capacity();
    int currentIndex = 0;
    for( int i = 0 ; i < length ; i++ ){
      int arrayLength = buffer.get();
      if( arrayLength != 0 ){
        int start = currentIndex;
        allocator.setArrayIndex( i , start , arrayLength );
        currentIndex += arrayLength;
      }
      else{
        allocator.setNull(i);
      }
    }
    allocator.setValueCount( length );
  }

  @Override
  public void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary ) throws IOException{
    parentNode.getChildNode( columnBinary.columnName ).disable();
  }

  public class ArrayCellManager implements ICellManager{

    private final ICell[] cellArray;

    public ArrayCellManager( final Spread spread , final IntBuffer buffer ){
      int length = buffer.capacity();
      cellArray = new ICell[length];
      int currentIndex = 0;
      for( int i = 0 ; i < length ; i++ ){
        int arrayLength = buffer.get();
        if( arrayLength != 0 ){
          int start = currentIndex;
          int end = start + arrayLength;
          cellArray[i] = new ArrayCell( new SpreadArrayLink( spread , i , start , end ) );
          currentIndex += arrayLength;
        }
      }
    }

    @Override
    public void add( final ICell cell , final int index ){
      throw new UnsupportedOperationException( "read only." );
    }

    @Override
    public ICell get( final int index , final ICell defaultCell ){
      if( cellArray.length <= index ){
        return defaultCell;
      }

      if( cellArray[index] == null ){
        return defaultCell;
      }
      return cellArray[index];
    }

    @Override
    public int getMaxIndex(){
      return cellArray.length - 1;
    }

    @Override
    public int size(){
      return cellArray.length;
    }

    @Override
    public void clear(){
    }

    @Override
    public void setIndex( final ICellIndex index ){
    }

    @Override
    public List<Integer> filter( final IFilter filter ) throws IOException{
      switch( filter.getFilterType() ){
        case NOT_NULL:
          List<Integer> notNullResult = new ArrayList<Integer>( cellArray.length );
          for( int i = 0 ; i < cellArray.length ; i++ ){
            if( cellArray[i] != null ){
              notNullResult.add( i );
            }
          }
          return notNullResult;
        case NULL:
          List<Integer> nullResult = new ArrayList<Integer>( cellArray.length );
          for( int i = 0 ; i < cellArray.length ; i++ ){
            if( cellArray == null ){
              nullResult.add( i );
            }
          }
          return nullResult;
        default:
          return null;
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveObjectArray( final IExpressionIndex indexList , final int start , final int length ){
      return new PrimitiveObject[length];
    }

    @Override
    public void setPrimitiveObjectArray( final IExpressionIndex indexList , final int start , final int length , final IMemoryAllocator allocator ){
      return;
    }

  }

  public class ArrayColumnManager implements IColumnManager{

    private final ColumnBinary columnBinary;
    private final IPrimitiveObjectConnector primitiveObjectConnector;
    private ArrayColumn arrayColumn;
    private boolean isCreate;

    public ArrayColumnManager( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
      this.columnBinary = columnBinary;
      this.primitiveObjectConnector = primitiveObjectConnector;
    }

    private void create() throws IOException{
      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      int decompressSize = compressor.getDecompressSize( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
      byte[] decompressBuffer = new byte[decompressSize];

      int binaryLength = compressor.decompressAndSet( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength , decompressBuffer );

      IntBuffer wrapBuffer = ByteBuffer.wrap( decompressBuffer , 0 , binaryLength ).asIntBuffer() ;

      arrayColumn = new ArrayColumn( columnBinary.columnName );
      Spread spread = new Spread( arrayColumn );
      for( ColumnBinary childColumnBinary : columnBinary.columnBinaryList ){
        IColumnBinaryMaker maker = FindColumnBinaryMaker.get( childColumnBinary.makerClassName );
        IColumn column = maker.toColumn( childColumnBinary , primitiveObjectConnector );
        column.setParentsColumn( arrayColumn );
        spread.addColumn( column );
      }
      spread.setRowCount( columnBinary.rowCount );

      arrayColumn.setSpread( spread );
      arrayColumn.setCellManager( new ArrayCellManager( spread , wrapBuffer ) );

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
      return arrayColumn;
    }

    @Override
    public List<String> getColumnKeys(){
      if( isCreate ){
        return arrayColumn.getColumnKeys();
      }
      else{
        return new ArrayList<String>();
      }
    }

    @Override
    public int getColumnSize(){
      if( isCreate ){
        return arrayColumn.getColumnSize();
      }
      else{
        return 1;
      }
    }
  }

}
