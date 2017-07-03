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
package jp.co.yahoo.dataplatform.mds.hadoop.hive;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;

import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.ArrayCell;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

import jp.co.yahoo.dataplatform.mds.hadoop.hive.io.ColumnAndIndex;
import jp.co.yahoo.dataplatform.mds.hadoop.hive.io.PrimitiveToWritableConverter;

public class MDSListObjectInspector implements SettableListObjectInspector{

  private final ObjectInspector valueObjectInspector;
  private final IGetField getField;

  public MDSListObjectInspector( final ListTypeInfo typeInfo ){
    valueObjectInspector = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( typeInfo.getListElementTypeInfo() ); 
    if( valueObjectInspector.getCategory() == ObjectInspector.Category.PRIMITIVE ){
      getField = new PrimitiveGetField( (PrimitiveObjectInspector)valueObjectInspector );
    }
    else if( valueObjectInspector.getCategory() == ObjectInspector.Category.UNION ){
      getField = new UnionGetField( (UnionTypeInfo)( typeInfo.getListElementTypeInfo() ) );
    }
    else{
      getField = new NestedGetField();
    }
  }

  private interface IGetField{

    public Object get( final ColumnAndIndex columnAndIndex , final ArrayCell arrayCell , final int index );

  }

  private static class PrimitiveGetField implements IGetField{
    private final PrimitiveObjectInspector inspector;

    public PrimitiveGetField( final PrimitiveObjectInspector inspector ){
      this.inspector = inspector;
    }

    @Override
    public Object get( final ColumnAndIndex columnAndIndex , final ArrayCell arrayCell , final int index ){
      try{
        return PrimitiveToWritableConverter.convert( inspector.getPrimitiveCategory() , arrayCell.getArrayRow( index ) );
      }catch( IOException e ){
        throw new RuntimeException( e );
      }
    }
  }

  private static class NestedGetField implements IGetField{

    @Override
    public Object get( final ColumnAndIndex columnAndIndex , final ArrayCell arrayCell , final int index ){
      int targetIndex = arrayCell.getStart() + index;
      return new ColumnAndIndex( columnAndIndex.column.getColumn(0) , targetIndex , columnAndIndex.columnIndex );
    }
  }

  private static class UnionGetField implements IGetField{

    private final UnionField unionField;

    public UnionGetField( final UnionTypeInfo unionTypeInfo ){
      unionField = new UnionField( unionTypeInfo );
    }

    @Override
    public Object get( final ColumnAndIndex columnAndIndex , final ArrayCell arrayCell , final int index ){
      int targetIndex = arrayCell.getStart() + index;
      return unionField.get( new ColumnAndIndex( columnAndIndex.column.getColumn(0) , targetIndex , columnAndIndex.columnIndex ) );
    }

  }

  @Override
  public ObjectInspector getListElementObjectInspector(){
    return valueObjectInspector;
  }

  @Override
  public Object getListElement( final Object object, final int index ){
    if( object instanceof ColumnAndIndex ){
      ColumnAndIndex columnAndIndex = (ColumnAndIndex) object;
      ICell cell = columnAndIndex.column.get( columnAndIndex.index );
      if( cell.getType() == ColumnType.ARRAY ){
        return getField.get( columnAndIndex , (ArrayCell)cell , index );
      }
      else{
        return null;
      }
    }
    else{
      return ( (List)object ).get( index );
    }
  }

  @Override
  public int getListLength( final Object object ){
    if( object == null ){
      return 0;
    }

    if( object instanceof ColumnAndIndex ){
      ColumnAndIndex columnAndIndex = (ColumnAndIndex) object;
      ICell cell = columnAndIndex.column.get( columnAndIndex.index );
      if( cell.getType() == ColumnType.ARRAY ){
        ArrayCell arrayCell = (ArrayCell)cell;
        return arrayCell.getEnd() - arrayCell.getStart();
      }
      return 0;
    }
    else{
      return ( (List)object ).size();
    }
  }

  @Override
  public List<?> getList( final Object object ){
    if( object instanceof ColumnAndIndex ){
      ColumnAndIndex columnAndIndex = (ColumnAndIndex) object;
      ICell cell = columnAndIndex.column.get( columnAndIndex.index );
      if( cell.getType() == ColumnType.ARRAY ){
        ArrayCell arrayCell = (ArrayCell)cell;
        List<Object> result = new ArrayList<Object>();
        int length = arrayCell.getEnd() - arrayCell.getStart();
        for( int i = 0 ; i < length ; i++ ){
          result.add( getField.get( columnAndIndex , arrayCell , i ) );
        }
        return result;
      }
      return null;
    }
    else{
      return (List)object;
    }
  }

  @Override
  public String getTypeName() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("array<");
    buffer.append( valueObjectInspector.getTypeName() );
    buffer.append(">");

    return buffer.toString();
  }

  @Override
  public Category getCategory(){
    return Category.LIST;
  }

  @Override
  public Object create( final int size ){
    List<Object> list = new ArrayList<Object>( size );
    for (int i = 0; i < size; i++) {
      list.add( null );
    }
    return list;
  }

  @Override
  public Object resize( final Object object ,  final int newSize ){
    ArrayList list = (ArrayList) object;
    list.ensureCapacity(newSize);
    while (list.size() < newSize) {
      list.add(null);
    }
    while (list.size() > newSize) {
      list.remove(list.size() - 1);
    }
    return list;
  }

  @Override
  public Object set( final Object object, final int index, final Object element ) {
    List<Object> list = (List<Object>) object;
    list.set( index , element );
    return list;
  }

}
