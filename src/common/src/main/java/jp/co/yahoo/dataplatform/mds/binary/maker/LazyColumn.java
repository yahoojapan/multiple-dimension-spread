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

import java.util.List;

import jp.co.yahoo.dataplatform.schema.design.IField;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ICellManager;
import jp.co.yahoo.dataplatform.mds.spread.column.NullColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.IFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.index.ICellIndex;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionIndex;

public class LazyColumn implements IColumn{

  private final ColumnType columnType;
  private final IColumnManager columnManager;
  private String columnName;
  private IColumn parentsColumn = NullColumn.getInstance();
  
  public LazyColumn( final String columnName , final ColumnType columnType , final IColumnManager columnManager ){
    this.columnName = columnName;
    this.columnType = columnType;
    this.columnManager = columnManager;
  }
 
  @Override
  public void setColumnName( final String columnName ){
    this.columnName = columnName;
  }

  @Override
  public String getColumnName(){
    return columnName;
  }

  @Override
  public ColumnType getColumnType(){
    return columnType;
  }

  @Override
  public void setParentsColumn( final IColumn parentsColumn ){
    this.parentsColumn = parentsColumn;
  }

  @Override
  public IColumn getParentsColumn(){
    return parentsColumn;
  }

  @Override
  public int add( final ColumnType type , final Object obj , final int index ) throws IOException{
    return columnManager.get().add( type , obj , index );
  }

  @Override
  public void addCell( final ColumnType type , final ICell cell , final int index ) throws IOException{
    columnManager.get().addCell( type , cell , index );
  }

  @Override
  public ICellManager getCellManager(){
    return columnManager.get().getCellManager();
  }

  @Override
  public void setCellManager( final ICellManager cellManager ){
    columnManager.get().setCellManager( cellManager );
  }

  @Override
  public ICell get( final int index ){
    return columnManager.get().get( index );
  }

  @Override
  public List<String> getColumnKeys(){
    return columnManager.getColumnKeys();
  }

  @Override
  public int getColumnSize(){
    return columnManager.getColumnSize();
  }

  @Override
  public List<IColumn> getListColumn(){
    return columnManager.get().getListColumn();
  }

  @Override
  public IColumn getColumn( final int index ){
    return columnManager.get().getColumn( index );
  }

  @Override
  public IColumn getColumn( final String columnName ){
    return columnManager.get().getColumn( columnName );
  }

  @Override
  public IColumn getColumn( final ColumnType type ){
    return columnManager.get().getColumn( type );
  }

  @Override
  public void setDefaultCell( final ICell defaultCell ){
    columnManager.get().setDefaultCell( defaultCell );
  }

  @Override
  public int size(){
    return columnManager.get().size();
  }

  @Override
  public IField getSchema() throws IOException {
    return columnManager.get().getSchema( getColumnName() );
  }

  @Override
  public IField getSchema( final String schemaName ) throws IOException{
    return columnManager.get().getSchema( schemaName );
  }

  @Override
  public void setIndex( final ICellIndex index ){
    columnManager.get().setIndex( index );
  }

  @Override
  public List<Integer> filter( final IFilter filter ) throws IOException{
    return columnManager.get().filter( filter );
  }

  @Override
  public PrimitiveObject[] getPrimitiveObjectArray( final IExpressionIndex indexList , final int start , final int length ){
    return columnManager.get().getPrimitiveObjectArray( indexList , start , length );
  }

  @Override
  public String toString(){
    return columnManager.get().toString();
  }

}
