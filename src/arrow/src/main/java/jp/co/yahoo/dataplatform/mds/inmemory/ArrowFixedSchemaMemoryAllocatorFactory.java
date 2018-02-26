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
package jp.co.yahoo.dataplatform.mds.inmemory;

import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

import jp.co.yahoo.dataplatform.schema.design.IField;
import jp.co.yahoo.dataplatform.schema.design.ArrayContainerField;
import jp.co.yahoo.dataplatform.schema.design.MapContainerField;
import jp.co.yahoo.dataplatform.schema.design.StructContainerField;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

public final class ArrowFixedSchemaMemoryAllocatorFactory{

  private ArrowFixedSchemaMemoryAllocatorFactory(){}

  public static IMemoryAllocator getFromMapVector( final IField schema , final String columnName , final BufferAllocator allocator , final MapVector vector ) throws IOException{
    switch( schema.getFieldType() ){
      case UNION:
        return NullMemoryAllocator.INSTANCE;
      case ARRAY:
        return new ArrowFixedSchemaArrayMemoryAllocator( (ArrayContainerField)schema , allocator , vector.addOrGetList( columnName ) );
      case MAP:
        NullableMapVector mapVector = vector.addOrGetMap( columnName );
        return new ArrowFixedSchemaMapMemoryAllocator( (MapContainerField)schema , allocator , mapVector );
      case STRUCT:
        NullableMapVector structVector = vector.addOrGetMap( columnName );
        return new ArrowFixedSchemaStructMemoryAllocator( (StructContainerField)schema , allocator , structVector );

      case BOOLEAN:
        return ArrowMemoryAllocatorFactory.getFromMapVector( ColumnType.BOOLEAN , columnName ,  allocator , vector );
      case BYTE:
        return ArrowMemoryAllocatorFactory.getFromMapVector( ColumnType.BYTE , columnName ,  allocator , vector );
      case SHORT:
        return ArrowMemoryAllocatorFactory.getFromMapVector( ColumnType.SHORT , columnName ,  allocator , vector );
      case INTEGER:
        return ArrowMemoryAllocatorFactory.getFromMapVector( ColumnType.INTEGER , columnName ,  allocator , vector );
      case LONG:
        return ArrowMemoryAllocatorFactory.getFromMapVector( ColumnType.LONG , columnName ,  allocator , vector );
      case FLOAT:
        return ArrowMemoryAllocatorFactory.getFromMapVector( ColumnType.FLOAT , columnName ,  allocator , vector );
      case DOUBLE:
        return ArrowMemoryAllocatorFactory.getFromMapVector( ColumnType.DOUBLE , columnName ,  allocator , vector );
      case STRING:
        return ArrowMemoryAllocatorFactory.getFromMapVector( ColumnType.STRING , columnName ,  allocator , vector );
      case BYTES:
        return ArrowMemoryAllocatorFactory.getFromMapVector( ColumnType.BYTES , columnName ,  allocator , vector );

      default:
        return NullMemoryAllocator.INSTANCE;
    }
  }

  public static IMemoryAllocator getFromListVector( final IField schema , final String columnName , final BufferAllocator allocator , final ListVector vector ) throws IOException{
    switch( schema.getFieldType() ){
      case UNION:
        return NullMemoryAllocator.INSTANCE;
      case ARRAY:
        AddOrGetResult<ListVector> listVector =  vector.addOrGetVector( new FieldType( true , ArrowType.List.INSTANCE , null , null ) );
        return new ArrowFixedSchemaArrayMemoryAllocator( (ArrayContainerField)schema , allocator , listVector.getVector() );
      case MAP:
        AddOrGetResult<MapVector> mapVector =  vector.addOrGetVector( new FieldType( true , ArrowType.Struct.INSTANCE , null , null ) );
        return new ArrowFixedSchemaMapMemoryAllocator( (MapContainerField)schema , allocator , mapVector.getVector() );
      case STRUCT:
        AddOrGetResult<MapVector> structVector =  vector.addOrGetVector( new FieldType( true , ArrowType.Struct.INSTANCE , null , null ) );
        return new ArrowFixedSchemaStructMemoryAllocator( (StructContainerField)schema , allocator , structVector.getVector() );

      case BOOLEAN:
        return ArrowMemoryAllocatorFactory.getFromListVector( ColumnType.BOOLEAN , columnName ,  allocator , vector );
      case BYTE:
        return ArrowMemoryAllocatorFactory.getFromListVector( ColumnType.BYTE , columnName ,  allocator , vector );
      case SHORT:
        return ArrowMemoryAllocatorFactory.getFromListVector( ColumnType.SHORT , columnName ,  allocator , vector );
      case INTEGER:
        return ArrowMemoryAllocatorFactory.getFromListVector( ColumnType.INTEGER , columnName ,  allocator , vector );
      case LONG:
        return ArrowMemoryAllocatorFactory.getFromListVector( ColumnType.LONG , columnName ,  allocator , vector );
      case FLOAT:
        return ArrowMemoryAllocatorFactory.getFromListVector( ColumnType.FLOAT , columnName ,  allocator , vector );
      case DOUBLE:
        return ArrowMemoryAllocatorFactory.getFromListVector( ColumnType.DOUBLE , columnName ,  allocator , vector );
      case STRING:
        return ArrowMemoryAllocatorFactory.getFromListVector( ColumnType.STRING , columnName ,  allocator , vector );
      case BYTES:
        return ArrowMemoryAllocatorFactory.getFromListVector( ColumnType.BYTES , columnName ,  allocator , vector );

      default:
        return NullMemoryAllocator.INSTANCE;
    }
  }

}
