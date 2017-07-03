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

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.SortedIntegerConverter;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveCell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

import jp.co.yahoo.dataplatform.schema.objects.IntegerObj;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveType;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;
import jp.co.yahoo.dataplatform.mds.compressor.FindCompressor;
import jp.co.yahoo.dataplatform.mds.binary.BinaryUtil;
import jp.co.yahoo.dataplatform.mds.binary.BinaryDump;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;

import static jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength.INT_LENGTH;

public class DumpIntegerColumnBinaryMaker implements IColumnBinaryMaker{

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column , final MakerCache makerCache ) throws IOException{
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    List<Integer> columnList = new ArrayList<Integer>();
    List<Integer> byteList = new ArrayList<Integer>();
    int rowCount = 0;
    for( int i = 0 ; i < column.size() ; i++ ){
      ICell cell = column.get(i);
      if( cell.getType() == ColumnType.NULL ){
        continue;
      }
      rowCount++;
      PrimitiveCell byteCell = (PrimitiveCell) cell;
      byteList.add( Integer.valueOf( byteCell.getRow().getInt() ) );
      columnList.add( Integer.valueOf( i ) );
    }

    byte[] columnBinaryRaw = BinaryUtil.toLengthBytesBinary( SortedIntegerConverter.getBinary( columnList ) );
    byte[] dicRawBinary = BinaryUtil.toLengthBytesBinary( BinaryDump.dumpInteger( byteList ) );

    byte[] binaryRaw = new byte[ columnBinaryRaw.length + dicRawBinary.length ];
    int offset = 0;
    System.arraycopy( columnBinaryRaw , 0 , binaryRaw , offset , columnBinaryRaw.length );
    offset += columnBinaryRaw.length;
    System.arraycopy( dicRawBinary , 0 , binaryRaw , offset , dicRawBinary.length );
    byte[] binary = currentConfig.compressorClass.compress( binaryRaw , 0 , binaryRaw.length );
    
    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.INTEGER , rowCount , binaryRaw.length , rowCount * INT_LENGTH , -1 , binary , 0 , binary.length , null );
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
    return new LazyColumn( columnBinary.columnName , columnBinary.columnType , new IntegerColumnManager( columnBinary , primitiveObjectConnector ) );
  }

  public class IntegerDicManager implements IDicManager{

    private final ICompressor compressor;
    private final IPrimitiveObjectConnector primitiveObjectConnector;
    private final byte[] dicBinary;
    private final int dicStart;
    private final int dicLength;

    private boolean isSet;
    private List<PrimitiveObject> dicList;

    public IntegerDicManager( final ICompressor compressor , final IPrimitiveObjectConnector primitiveObjectConnector , final byte[] dicBinary , final int dicStart , final int dicLength ){
      this.compressor = compressor;
      this.dicBinary = dicBinary;
      this.dicStart = dicStart;
      this.dicLength = dicLength;
      this.primitiveObjectConnector = primitiveObjectConnector;
    }

    private void decompress() throws IOException{
      if( isSet ){
        return;
      }
      dicList = new ArrayList<PrimitiveObject>();
      for( Integer data : BinaryDump.binaryToIntegerList( dicBinary , dicStart , dicLength ) ){
        dicList.add( primitiveObjectConnector.convert( PrimitiveType.INTEGER , new IntegerObj( data.intValue() ) ) );
      }
      isSet = true;
    }

    @Override
    public PrimitiveObject get( final int index ) throws IOException{
      decompress();
      return dicList.get( index );
    }

    @Override
    public int getDicSize() throws IOException{
      decompress();
      return dicList.size();
    }
  }

  public class IntegerColumnManager implements IColumnManager{

    private final IPrimitiveObjectConnector primitiveObjectConnector;
    private final ColumnBinary columnBinary;
    private PrimitiveColumn column;
    private boolean isCreate;

    public IntegerColumnManager( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
      this.columnBinary = columnBinary;
      this.primitiveObjectConnector = primitiveObjectConnector;
    }

    private void create() throws IOException{
      if( isCreate ){
        return;
      }
      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] binary = compressor.decompress( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary );
      int offset = 0;
      int columnBinaryLength = wrapBuffer.getInt( offset );
      offset += INT_LENGTH;
      int columnBinaryStart = offset;
      offset += columnBinaryLength;

      int dicBinaryLength = wrapBuffer.getInt( offset );
      offset += INT_LENGTH;
      int dicBinaryStart = offset;
      offset += dicBinaryLength;

      List<Integer> columnIndexList = SortedIntegerConverter.getIntegerList( binary , columnBinaryStart , columnBinaryLength );
      column = new PrimitiveColumn( columnBinary.columnType , columnBinary.columnName );
      IDicManager dicManager = new IntegerDicManager( compressor , primitiveObjectConnector , binary , dicBinaryStart , dicBinaryLength );
      int dicIndex = 0;
      for( Integer index : columnIndexList ){
        column.addCell( columnBinary.columnType , new LazyCell( ColumnType.INTEGER , dicManager , dicIndex ) , index );
        dicIndex++;
      }

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
