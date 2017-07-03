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

import jp.co.yahoo.dataplatform.mds.hadoop.hive.io.ColumnAndIndex;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObject;

import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

import jp.co.yahoo.dataplatform.mds.hadoop.hive.io.PrimitiveToWritableConverter;

public class UnionField{

  private final Map<ColumnType,IGetUnionObject> columnTypeMap = new HashMap<ColumnType,IGetUnionObject>();

  private interface IGetUnionObject{

    public UnionObject get( final ColumnAndIndex columnAndIndex );

    public ColumnType getColumnType();

  }

  private class PrimitiveGetUnionObject implements IGetUnionObject{

    private final byte tag;
    private final PrimitiveObjectInspector inspector;
    private final ColumnType columnType;

    public PrimitiveGetUnionObject( final byte tag , final PrimitiveObjectInspector inspector , final ColumnType columnType ){
      this.tag = tag;
      this.inspector = inspector;
      this.columnType = columnType;
    }

    @Override
    public UnionObject get( final ColumnAndIndex columnAndIndex ){
      try{
        return new StandardUnion( tag , PrimitiveToWritableConverter.convert( inspector.getPrimitiveCategory() , columnAndIndex.column.get( columnAndIndex.index ) ) );
      }catch( IOException e ){
        return null;
      }
    }

    @Override
    public ColumnType getColumnType(){
      return columnType;
    }

  }

  private class NestedGetUnionObject implements IGetUnionObject{

    private final byte tag;
    private final ColumnType columnType;

    public NestedGetUnionObject( final byte tag , final ColumnType columnType ){
      this.tag = tag;
      this.columnType = columnType;
    }

    @Override
    public UnionObject get( final ColumnAndIndex columnAndIndex ){
      return new StandardUnion( tag , columnAndIndex );
    }

    @Override
    public ColumnType getColumnType(){
      return columnType;
    }
  }

  private class NullGetUnionObject implements IGetUnionObject{

    private final byte tag;

    public NullGetUnionObject( final byte tag ){
      this.tag = tag;
    }

    @Override
    public UnionObject get( final ColumnAndIndex columnAndIndex ){
      return null;
    }

    @Override
    public ColumnType getColumnType(){
      return ColumnType.NULL;
    }
  }

  private IGetUnionObject craeteGetUnionObject( final byte tag , final TypeInfo typeInfo ){
    switch ( typeInfo.getCategory() ){
      case STRUCT:
        return new NestedGetUnionObject( tag , ColumnType.SPREAD );
      case MAP:
        return new NestedGetUnionObject( tag , ColumnType.SPREAD );
      case LIST:
        return new NestedGetUnionObject( tag , ColumnType.ARRAY );
      case UNION:
        return new NullGetUnionObject( tag );
      case PRIMITIVE:
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo)typeInfo;
        PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector)( MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( typeInfo ) );
        switch( primitiveTypeInfo.getPrimitiveCategory() ){
          case STRING:
            return new PrimitiveGetUnionObject( tag , primitiveObjectInspector , ColumnType.STRING );
          case BINARY:
            return new PrimitiveGetUnionObject( tag , primitiveObjectInspector , ColumnType.BYTES );
          case BOOLEAN:
            return new PrimitiveGetUnionObject( tag , primitiveObjectInspector , ColumnType.BOOLEAN );
          case BYTE:
            return new PrimitiveGetUnionObject( tag , primitiveObjectInspector , ColumnType.BYTE );
          case DOUBLE:
            return new PrimitiveGetUnionObject( tag , primitiveObjectInspector , ColumnType.DOUBLE );
          case FLOAT:
            return new PrimitiveGetUnionObject( tag , primitiveObjectInspector , ColumnType.FLOAT );
          case INT:
            return new PrimitiveGetUnionObject( tag , primitiveObjectInspector , ColumnType.INTEGER );
          case LONG:
            return new PrimitiveGetUnionObject( tag , primitiveObjectInspector , ColumnType.LONG );
          case SHORT:
            return new PrimitiveGetUnionObject( tag , primitiveObjectInspector , ColumnType.SHORT );
          case DATE:
          case DECIMAL:
          case TIMESTAMP:
          case VOID:
          default:
            return new NullGetUnionObject( tag );
        }
      default:
        return new NullGetUnionObject( tag );
    }
  }

  public UnionField( final UnionTypeInfo typeInfo ){
    byte tag = (byte)0;
    
    for( TypeInfo childTypeInfo : typeInfo.getAllUnionObjectTypeInfos() ){
      IGetUnionObject field = craeteGetUnionObject( tag , childTypeInfo );
      columnTypeMap.put( field.getColumnType() , field );
      tag++;
    }
  }

  public UnionObject get( final ColumnAndIndex columnAndIndex ){
    IGetUnionObject field = columnTypeMap.get( columnAndIndex.column.get( columnAndIndex.index ).getType() );
    if( field == null ){
      return null;
    }
    return field.get( columnAndIndex );
  }

}
