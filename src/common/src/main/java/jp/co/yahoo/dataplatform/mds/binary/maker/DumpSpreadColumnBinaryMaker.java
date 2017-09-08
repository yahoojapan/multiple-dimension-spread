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

import java.util.List;
import java.util.ArrayList;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.spread.column.SpreadColumn;
import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.FindColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class DumpSpreadColumnBinaryMaker implements IColumnBinaryMaker{

  @Override
  public ColumnBinary toBinary(final ColumnBinaryMakerConfig commonConfig , final ColumnBinaryMakerCustomConfigNode currentConfigNode , final IColumn column , final MakerCache makerCache ) throws IOException{
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if( currentConfigNode != null ){
      currentConfig = currentConfigNode.getCurrentConfig();
    }

    List<IColumn> childColumnList = column.getListColumn();
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
      columnBinaryList.add( maker.toBinary( commonConfig , childNode , childColumn , makerCache.getChild( childColumn.getColumnName() ) ) );
    }
    
    return new ColumnBinary( this.getClass().getName() , currentConfig.compressorClass.getClass().getName() , column.getColumnName() , ColumnType.SPREAD , column.size() , 0 , 0 , -1 , new byte[0] , 0 , 0 , columnBinaryList );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ){
    return 0;
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector  ) throws IOException{
    return new LazyColumn( columnBinary.columnName , columnBinary.columnType , new SpreadColumnManager( columnBinary , primitiveObjectConnector ) );
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

  public class SpreadColumnManager implements IColumnManager{

    private final List<String> keyList;
    private final ColumnBinary columnBinary;
    private final IPrimitiveObjectConnector primitiveObjectConnector;
    private SpreadColumn spreadColumn;
    private boolean isCreate;

    public SpreadColumnManager( final ColumnBinary columnBinary , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
      this.columnBinary = columnBinary;
      this.primitiveObjectConnector = primitiveObjectConnector;
      keyList = new ArrayList<String>();
      for( ColumnBinary childColumnBinary : columnBinary.columnBinaryList ){
        keyList.add( childColumnBinary.columnName );
      }
    }

    private void create() throws IOException{
      if( isCreate ){
        return;
      }

      spreadColumn = new SpreadColumn( columnBinary.columnName );
      Spread spread = new Spread();
      for( ColumnBinary childColumnBinary : columnBinary.columnBinaryList ){
        IColumnBinaryMaker maker = FindColumnBinaryMaker.get( childColumnBinary.makerClassName );
        IColumn column = maker.toColumn( childColumnBinary , primitiveObjectConnector );
        column.setParentsColumn( spreadColumn );
        spread.addColumn( column );
      }
      spread.setRowCount( columnBinary.rowCount );

      spreadColumn.setSpread( spread );

      isCreate = true;
    }

    @Override
    public IColumn get(){
      try{
        create();
      }catch( IOException e ){
        throw new UncheckedIOException( e );
      }
      return spreadColumn;
    }

    @Override
    public List<String> getColumnKeys(){
      if( isCreate ){
        return spreadColumn.getColumnKeys();
      }
      else{
        return keyList;
      }
    }

    @Override
    public int getColumnSize(){
      if( isCreate ){
        return spreadColumn.getColumnSize();
      }
      else{
        return keyList.size();
      }
    }
  }

}
