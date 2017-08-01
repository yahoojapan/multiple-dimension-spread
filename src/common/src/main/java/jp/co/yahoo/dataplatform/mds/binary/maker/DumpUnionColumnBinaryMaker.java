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
import java.util.Map;
import java.util.HashMap;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.FindColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.compressor.FindCompressor;
import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;
import jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.UnionColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnTypeFactory;
import jp.co.yahoo.dataplatform.mds.binary.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;


public class DumpUnionColumnBinaryMaker implements IColumnBinaryMaker{

  public enum MargeType{
    INTEGER,
    FLOAT,
    MIX
  }

  public static MargeType getMargeType( final ColumnType columnType ){
    switch( columnType ){
      case BYTE:
      case SHORT:
      case INTEGER:
      case LONG:
        return MargeType.INTEGER;
      case FLOAT:
      case DOUBLE:
        return MargeType.FLOAT;
      default:
        return MargeType.MIX;
    }
  }

  private MargeType checkSameAllColumnType( final List<IColumn> columnList , final MargeType margeType ){
    for( IColumn column : columnList ){
      if( ! ( getMargeType( column.getColumnType() ) == margeType ) ){
        return MargeType.MIX;
      }
    }
    return margeType;
  }

  public MargeType checkMargeType( final List<IColumn> columnList ){
    switch( getMargeType( columnList.get(0).getColumnType() ) ){
      case INTEGER:
        return checkSameAllColumnType( columnList , MargeType.INTEGER );
      case FLOAT:
        return checkSameAllColumnType( columnList , MargeType.FLOAT );
      default:
        return MargeType.MIX;
    }
  }

  private ColumnBinary margeColumn(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column , final MakerCache makerCache , final List<IColumn> childColumnList ) throws IOException {
    int max = -1;
    IColumnBinaryMaker maker = null;
    for( IColumn childColumn : childColumnList ){
      ColumnType columnType = childColumn.getColumnType();
      int columnSize = ColumnTypeFactory.getColumnTypeToPrimitiveByteSize( columnType , null );
      if( max < columnSize ){
        max = columnSize;
        maker = commonConfig.getColumnMaker( columnType );
        if( currentConfigNode != null ){
          maker = currentConfigNode.getCurrentConfig().getColumnMaker( columnType );
        }
      }
    }
    return maker.toBinary( commonConfig , currentConfigNode , column , makerCache );
  }

  @Override
  public ColumnBinary toBinary( final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column , final MakerCache makerCache ) throws IOException{
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    List<IColumn> childColumnList = column.getListColumn();
    MargeType margeType = checkMargeType( childColumnList );
    if( margeType != MargeType.MIX ){
      return margeColumn( commonConfig , currentConfigNode , column , makerCache , childColumnList );
    }
    List<ColumnBinary> columnBinaryList = new ArrayList<ColumnBinary>();
    for( IColumn childColumn : childColumnList ){
      ColumnBinaryMakerCustomConfigNode childNode = null;
      IColumnBinaryMaker maker = commonConfig.getColumnMaker( childColumn.getColumnType() );
      if( currentConfigNode != null ){
        childNode = currentConfigNode.getChildConfigNode( childColumn.getColumnName() );
        if( childNode != null ){
          maker = childNode.getCurrentConfig().getColumnMaker( childColumn.getColumnType() );
        }
      }
      columnBinaryList.add( maker.toBinary( commonConfig , childNode , childColumn , makerCache.getChild( childColumn.getColumnType().toString() ) ) );
    }

    byte[] rawBinary = new byte[ column.size() ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( rawBinary );
    for( int i = 0 ; i < column.size() ; i++ ){
      wrapBuffer.put( ColumnTypeFactory.getColumnTypeByte( column.get(i).getType() ) );
    }

    byte[] compressData = currentConfig.compressorClass.compress( rawBinary , 0 , rawBinary.length );
    
    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.UNION , column.size() , rawBinary.length , column.size() * PrimitiveByteLength.BYTE_LENGTH , -1 , compressData , 0 , compressData.length , columnBinaryList );
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
    return new LazyColumn( columnBinary.columnName , columnBinary.columnType , new UnionColumnManager( columnBinary , primitiveObjectConnector ) );
  }

  @Override
  public void loadInMemoryStorage( final ColumnBinary columnBinary , final IMemoryAllocator allocator ) throws IOException{
    int maxValueCount = 0;
    for( ColumnBinary childColumnBinary : columnBinary.columnBinaryList ){
      IColumnBinaryMaker maker = FindColumnBinaryMaker.get( childColumnBinary.makerClassName );
      IMemoryAllocator childAllocator = allocator.getChild( childColumnBinary.columnName , childColumnBinary.columnType );
      maker.loadInMemoryStorage( childColumnBinary , childAllocator );
      if( maxValueCount < childAllocator.getValueCount() ){
        maxValueCount = childAllocator.getValueCount();
      }
    }
    allocator.setValueCount( maxValueCount );
  }

  @Override
  public void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary ) throws IOException{
    parentNode.getChildNode( columnBinary.columnName ).disable();
  }

  public class UnionColumnManager implements IColumnManager{

    private final ColumnBinary columnBinary;
    private final IPrimitiveObjectConnector primitiveObjectConnector;
    private UnionColumn unionColumn;
    private boolean isCreate;

    public UnionColumnManager( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
      this.columnBinary = columnBinary;
      this.primitiveObjectConnector = primitiveObjectConnector;
    }

    private void create() throws IOException{
      if( isCreate ){
        return;
      }

      Map<ColumnType,IColumn> columnContainer = new HashMap<ColumnType,IColumn>();
      unionColumn = new UnionColumn( columnBinary.columnName , columnContainer );

      for( ColumnBinary childColumnBinary : columnBinary.columnBinaryList ){
        IColumnBinaryMaker maker = FindColumnBinaryMaker.get( childColumnBinary.makerClassName );
        IColumn column = maker.toColumn( childColumnBinary , primitiveObjectConnector );
        column.setParentsColumn( unionColumn );
        unionColumn.setColumn( column );
        columnContainer.put( column.getColumnType() , column );
      }


      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] cellBinary = compressor.decompress( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
      ByteBuffer wrapBuffer = ByteBuffer.wrap( cellBinary );
      for( int i = 0 ; i < cellBinary.length ; i++ ){
        ColumnType columnType = ColumnTypeFactory.getColumnTypeFromByte( wrapBuffer.get() );
        if( columnContainer.containsKey( columnType ) ){
          unionColumn.addCell( columnType , columnContainer.get( columnType ).get( i ) , i );
        }
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
      return unionColumn;
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
