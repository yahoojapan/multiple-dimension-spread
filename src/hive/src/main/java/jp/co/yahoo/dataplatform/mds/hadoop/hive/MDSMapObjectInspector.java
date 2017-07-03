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

import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableMapObjectInspector;

import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;

import jp.co.yahoo.dataplatform.mds.hadoop.hive.io.ColumnAndIndex;
import jp.co.yahoo.dataplatform.mds.hadoop.hive.io.PrimitiveToWritableConverter;

public class MDSMapObjectInspector implements SettableMapObjectInspector{

  private final StringObjectInspector keyObjectInspector;
  private final ObjectInspector valueObjectInspector;
  private final IGetField getField;

  public MDSMapObjectInspector( final MapTypeInfo typeInfo ){
    TypeInfo keyTypeInfo = typeInfo.getMapKeyTypeInfo();
    if( keyTypeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE && ( (PrimitiveTypeInfo)keyTypeInfo ).getPrimitiveCategory() == PrimitiveCategory.STRING ){
      keyObjectInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }
    else{
      throw new RuntimeException( "Map key type is string only." );
    }

    valueObjectInspector = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( typeInfo.getMapValueTypeInfo() ); 

    if( valueObjectInspector.getCategory() == ObjectInspector.Category.PRIMITIVE ){
      getField = new PrimitiveGetField( (PrimitiveObjectInspector)valueObjectInspector );
    }
    else if( valueObjectInspector.getCategory() == ObjectInspector.Category.UNION ){
      getField = new UnionGetField( (UnionTypeInfo)( typeInfo.getMapValueTypeInfo() ) );
    }
    else{
      getField = new NestedGetField();
    }
  }

  private interface IGetField{

    public Object get( final IColumn childColumn , final int index , final int columnIndex );

  }

  private static class PrimitiveGetField implements IGetField{
    private final PrimitiveObjectInspector inspector;

    public PrimitiveGetField( final PrimitiveObjectInspector inspector ){
      this.inspector = inspector;
    }

    @Override
    public Object get( final IColumn childColumn , final int index , final int columnIndex ){
      try{
        return PrimitiveToWritableConverter.convert( inspector.getPrimitiveCategory() , childColumn.get( index ) );
      }catch( IOException e ){
        throw new RuntimeException( e );
      }
    }
  }

  private static class NestedGetField implements IGetField{

    @Override
    public Object get( final IColumn childColumn , final int index , final int columnIndex ){
      return new ColumnAndIndex( childColumn , index , columnIndex );
    }
  }

  private static class UnionGetField implements IGetField{

    private final UnionField unionField;

    public UnionGetField( final UnionTypeInfo unionTypeInfo ){
      unionField = new UnionField( unionTypeInfo );
    }

    @Override
    public Object get( final IColumn childColumn , final int index , final int columnIndex ){
      return unionField.get( new ColumnAndIndex( childColumn , index , columnIndex ) );
    }

  }

  @Override
  public ObjectInspector getMapKeyObjectInspector(){
    return keyObjectInspector;
  }

  @Override
  public ObjectInspector getMapValueObjectInspector(){
    return valueObjectInspector;
  }

  @Override
  public Object getMapValueElement( final Object object, final Object key ){
    Map map = (Map)object;
    return map.get(key);
  }

  @Override
  public Map<?, ?> getMap( final Object object ){
    if( object instanceof ColumnAndIndex ){
      ColumnAndIndex columnAndIndex = (ColumnAndIndex) object;
      int childColumnSize = columnAndIndex.column.getColumnSize();
      Map<Object,Object> result = new HashMap<Object, Object>( childColumnSize );
      for( int i = 0 ; i < childColumnSize ; i++ ){
        IColumn childColumn = columnAndIndex.column.getColumn(i);
        result.put( childColumn.getColumnName() , getField.get( childColumn , columnAndIndex.index , columnAndIndex.columnIndex ) );
      }
      return result;
    }
    else{
      return (Map<?,?>)object;
    }
  }

  @Override
  public int getMapSize( final Object object ){
    return ((Map)object).size();
  }

  @Override
  public String getTypeName() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("map<");
    buffer.append( keyObjectInspector.getTypeName() );
    buffer.append(",");
    buffer.append( valueObjectInspector.getTypeName() );
    buffer.append(">");

    return buffer.toString();
  }

  @Override
  public Category getCategory(){
    return Category.MAP;
  }

  @Override
  public Object create(){
    return new HashMap<Object, Object>();
  }

  @Override
  public Object put( final Object map, final Object key,  final Object value ){
    ( (Map<Object, Object>)map ).put( key, value );
    return map;
  }

  @Override
  public Object remove( final Object map , final Object key ){
    ( (Map<Object, Object>)map ).remove(key);
    return map;
  }

  @Override
  public Object clear( final Object map ){
    ( (Map<Object, Object>)map ).clear();
    return map;
  }

}
