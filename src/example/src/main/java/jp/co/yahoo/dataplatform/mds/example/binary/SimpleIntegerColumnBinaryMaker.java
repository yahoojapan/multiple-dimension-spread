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
package jp.co.yahoo.dataplatform.mds.example.binary;

import java.io.IOException;
import java.nio.ByteBuffer;

import jp.co.yahoo.dataplatform.schema.objects.IntegerObj;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.compressor.FindCompressor;
import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;
import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveCell;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;

public class SimpleIntegerColumnBinaryMaker implements IColumnBinaryMaker{

  @Override
  public ColumnBinary toBinary( final ColumnBinaryMakerConfig commonConfig , 
    final ColumnBinaryMakerCustomConfigNode currentConfigNode , 
    final IColumn column ) throws IOException{

    // Use common settings. If there is a setting unique to this column, use that setting.
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }

    // Calculate buffer size from column size.
    // In this example, 0 and 1 are represented by bytes in the expression of NULL.
    // Therefore, the buffer size is the sum of the byte size used to represent NULL and the Int byte size representing Int and the number of columns.
    int nullByteLength = column.size() * Byte.BYTES;
    int intBinaryLength = column.size() * Integer.BYTES;
    byte[] buffer = new byte[ Integer.BYTES + nullByteLength + intBinaryLength ];

    // Substitute numeric and NULL information into buffer
    ByteBuffer.wrap( buffer , 0 , Integer.BYTES ).putInt( column.size() );
    ByteBuffer nullBuffer = ByteBuffer.wrap( buffer , Integer.BYTES , column.size() );
    ByteBuffer intBuffer = ByteBuffer.wrap( buffer , Integer.BYTES + nullByteLength , intBinaryLength );
    int rows = 0;
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      if( cell.getType() == ColumnType.NULL ){
        nullBuffer.put( (byte)1 );
        intBuffer.putInt( 0 );
      }
      else{
        nullBuffer.put( (byte)0 );
        int num = ( (PrimitiveCell)cell ).getRow().getInt();
        intBuffer.putInt( num );
        rows++;
      }
    }

    // Compress buffer
    byte[] compressBinary = currentConfig.compressorClass.compress( buffer , 0 , buffer.length );

    // Create a new ColumnBinary
    return new ColumnBinary(
      this.getClass().getName(),
      currentConfig.compressorClass.getClass().getName(),
      column.getColumnName(),
      ColumnType.INTEGER,
      rows,
      compressBinary.length,
      intBinaryLength,
      -1,
      compressBinary,
      0,
      compressBinary.length,
      null
    );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ){
    int columnSize = analizeResult.getColumnSize();
    int nullByteLength = columnSize * Byte.BYTES;
    int intBinaryLength = columnSize * Integer.BYTES;
    return Byte.BYTES * columnSize + columnSize * Integer.BYTES;
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException{
    // Decompress binary
    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );

    int columnSize = ByteBuffer.wrap( binary ).getInt();
    int nullByteLength = Byte.BYTES * columnSize;
    int intBinaryLength = Integer.BYTES * columnSize;
    ByteBuffer nullBuffer = ByteBuffer.wrap( binary , Integer.BYTES , nullByteLength );
    ByteBuffer intBuffer = ByteBuffer.wrap( binary , Integer.BYTES + nullByteLength , intBinaryLength );

    // Column creation and data set.
    IColumn column = new PrimitiveColumn( ColumnType.INTEGER , columnBinary.columnName );
    for( int i = 0 ; i < columnSize ; i++ ){
      byte isNull = nullBuffer.get();
      int number = intBuffer.getInt();
      if( isNull == (byte)0 ){
        column.add( ColumnType.INTEGER , new IntegerObj( number ) , i );
      }
    }
    return column;
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );

    int columnSize = ByteBuffer.wrap( binary ).getInt();
    int nullByteLength = Byte.BYTES * columnSize;
    int intBinaryLength = Integer.BYTES * columnSize;
    ByteBuffer nullBuffer = ByteBuffer.wrap( binary , Integer.BYTES , nullByteLength );
    ByteBuffer intBuffer = ByteBuffer.wrap( binary , Integer.BYTES + nullByteLength , intBinaryLength );

    // Load data structure using IMemoryAllocator.
    for( int i = 0 ; i < columnSize ; i++ ){
      byte isNull = nullBuffer.get();
      int number = intBuffer.getInt();
      if( isNull == (byte)0 ){
        allocator.setInteger( i , number );
      }
      else{
        allocator.setNull( i );
      }
    }

    // Finally, set the size of the column. Load processing needs to fill in NULL value.
    allocator.setValueCount( columnSize );
  }

  @Override
  public void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary , final int spreadIndex ) throws IOException{
    // In the example of this example, since it has no index, it is invalidated
    parentNode.getChildNode( columnBinary.columnName ).disable();
  }

}
