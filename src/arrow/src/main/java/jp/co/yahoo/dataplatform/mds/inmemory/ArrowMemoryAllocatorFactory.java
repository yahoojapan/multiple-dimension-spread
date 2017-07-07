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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.arrow.vector.NullableVarBinaryVector;
import org.apache.arrow.vector.NullableBitVector;
import org.apache.arrow.vector.NullableUInt1Vector;
import org.apache.arrow.vector.NullableUInt2Vector;
import org.apache.arrow.vector.NullableUInt4Vector;
import org.apache.arrow.vector.NullableUInt8Vector;
import org.apache.arrow.vector.NullableFloat4Vector;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.FieldType;

import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

public final class ArrowMemoryAllocatorFactory{

  private ArrowMemoryAllocatorFactory(){}

  public static IMemoryAllocator getFromMapVector( final ColumnType columnType , final String columnName , final BufferAllocator allocator , final MapVector vector ){
    switch( columnType ){
      case UNION:
        return new ArrowUnionMemoryAllocator( allocator , vector.addOrGetUnion( columnName ) );
      case ARRAY:
        return new ArrowArrayMemoryAllocator( allocator , vector.addOrGetList( columnName ) );
      case SPREAD:
        return new ArrowMapMemoryAllocator( allocator , vector.addOrGetMap( columnName ) );

      case BOOLEAN:
        NullableBitVector bitVector =  vector.addOrGet( columnName , new FieldType( true , ArrowType.Bool.INSTANCE , null , null ) , NullableBitVector.class );
        return new ArrowBooleanMemoryAllocator( bitVector );
      case BYTE:
        NullableUInt1Vector byteVector =  vector.addOrGet( columnName , new FieldType( true , new ArrowType.Int( 8 , false ) , null , null ) , NullableUInt1Vector.class );
        return new ArrowByteMemoryAllocator( byteVector );
      case SHORT:
        NullableUInt2Vector shortVector =  vector.addOrGet( columnName , new FieldType( true , new ArrowType.Int( 16 , false ) , null , null ) , NullableUInt2Vector.class );
        return new ArrowShortMemoryAllocator( shortVector );
      case INTEGER:
        NullableUInt4Vector integerVector =  vector.addOrGet( columnName , new FieldType( true , new ArrowType.Int( 32 , false ) , null , null ) , NullableUInt4Vector.class );
        return new ArrowIntegerMemoryAllocator( integerVector );
      case LONG:
        NullableUInt8Vector longVector =  vector.addOrGet( columnName , new FieldType( true , new ArrowType.Int( 64 , false ) , null , null ) , NullableUInt8Vector.class );
        return new ArrowLongMemoryAllocator( longVector );
      case FLOAT:
        NullableFloat4Vector floatVector =  vector.addOrGet( columnName , new FieldType( true , new ArrowType.FloatingPoint( FloatingPointPrecision.SINGLE ) , null , null ) , NullableFloat4Vector.class );
        return new ArrowFloatMemoryAllocator( floatVector );
      case DOUBLE:
        NullableFloat8Vector doubleVector =  vector.addOrGet( columnName , new FieldType( true , new ArrowType.FloatingPoint( FloatingPointPrecision.DOUBLE ) , null , null ) , NullableFloat8Vector.class );
        return new ArrowDoubleMemoryAllocator( doubleVector );
      case STRING:
        NullableVarCharVector charVector =  vector.addOrGet( columnName , new FieldType( true , ArrowType.Utf8.INSTANCE , null , null ) , NullableVarCharVector.class );
        return new ArrowStringMemoryAllocator( charVector );
      case BYTES:
        NullableVarBinaryVector binaryVector =  vector.addOrGet( columnName , new FieldType( true , ArrowType.Binary.INSTANCE , null , null ) , NullableVarBinaryVector.class );
        return new ArrowBytesMemoryAllocator( binaryVector );

      case NULL:
      case EMPTY_ARRAY:
      case EMPTY_SPREAD:
      default:
        return ArrowNullMemoryAllocator.getInstance();
    }
  }

  public static IMemoryAllocator getFromListVector( final ColumnType columnType , final String columnName , final BufferAllocator allocator , final ListVector vector ){
    switch( columnType ){
      case UNION:
        AddOrGetResult<UnionVector> unionVector =  vector.addOrGetVector( new FieldType( true , MinorType.UNION.getType() , null , null ) );
        return new ArrowUnionMemoryAllocator( allocator , unionVector.getVector() );
      case ARRAY:
        AddOrGetResult<ListVector> listVector =  vector.addOrGetVector( new FieldType( true , ArrowType.List.INSTANCE , null , null ) );
        return new ArrowArrayMemoryAllocator( allocator , listVector.getVector() );
      case SPREAD:
        AddOrGetResult<MapVector> mapVector =  vector.addOrGetVector( new FieldType( true , ArrowType.Struct.INSTANCE , null , null ) );
        return new ArrowMapMemoryAllocator( allocator , mapVector.getVector() );

      case BOOLEAN:
        AddOrGetResult<NullableBitVector> bitVector =  vector.addOrGetVector( new FieldType( true , ArrowType.Bool.INSTANCE , null , null ) );
        return new ArrowBooleanMemoryAllocator( bitVector.getVector() );
      case BYTE:
        AddOrGetResult<NullableUInt1Vector> byteVector =  vector.addOrGetVector( new FieldType( true , new ArrowType.Int( 8 , false ) , null , null ) );
        return new ArrowByteMemoryAllocator( byteVector.getVector() );
      case SHORT:
        AddOrGetResult<NullableUInt2Vector> shortVector =  vector.addOrGetVector( new FieldType( true , new ArrowType.Int( 16 , false ) , null , null ) );
        return new ArrowShortMemoryAllocator( shortVector.getVector() );
      case INTEGER:
        AddOrGetResult<NullableUInt4Vector> integerVector =  vector.addOrGetVector( new FieldType( true , new ArrowType.Int( 32 , false ) , null , null ) );
        return new ArrowIntegerMemoryAllocator( integerVector.getVector() );
      case LONG:
        AddOrGetResult<NullableUInt8Vector> longVector =  vector.addOrGetVector( new FieldType( true , new ArrowType.Int( 64 , false ) , null , null ) );
        return new ArrowLongMemoryAllocator( longVector.getVector() );
      case FLOAT:
        AddOrGetResult<NullableFloat4Vector> floatVector =  vector.addOrGetVector( new FieldType( true , new ArrowType.FloatingPoint( FloatingPointPrecision.HALF ) , null , null ) );
        return new ArrowFloatMemoryAllocator( floatVector.getVector() );
      case DOUBLE:
        AddOrGetResult<NullableFloat8Vector> doubleVector =  vector.addOrGetVector( new FieldType( true , new ArrowType.FloatingPoint( FloatingPointPrecision.DOUBLE ) , null , null ) );
        return new ArrowDoubleMemoryAllocator( doubleVector.getVector() );
      case STRING:
        AddOrGetResult<NullableVarCharVector> charVector =  vector.addOrGetVector( new FieldType( true , ArrowType.Utf8.INSTANCE , null , null ) );
        return new ArrowStringMemoryAllocator( charVector.getVector() );
      case BYTES:
        AddOrGetResult<NullableVarBinaryVector> binaryVector =  vector.addOrGetVector( new FieldType( true , ArrowType.Binary.INSTANCE , null , null ) );
        return new ArrowBytesMemoryAllocator( binaryVector.getVector() );

      case NULL:
      case EMPTY_ARRAY:
      case EMPTY_SPREAD:
      default:
        return ArrowNullMemoryAllocator.getInstance();
    }
  }

  public static IMemoryAllocator getFromUnionVector( final ColumnType columnType , final String columnName , final BufferAllocator allocator , final UnionVector vector ){
    switch( columnType ){
      case UNION:
        return ArrowNullMemoryAllocator.getInstance();
      case ARRAY:
        return new ArrowArrayMemoryAllocator( allocator , vector.getList() );
      case SPREAD:
        return new ArrowMapMemoryAllocator( allocator , vector.getMap() );

      case BOOLEAN:
        return new ArrowBooleanMemoryAllocator( vector.getBitVector() );
      case BYTE:
        return new ArrowByteMemoryAllocator( vector.getUInt1Vector() );
      case SHORT:
        return new ArrowShortMemoryAllocator( vector.getUInt2Vector() );
      case INTEGER:
        return new ArrowIntegerMemoryAllocator( vector.getUInt4Vector() );
      case LONG:
        return new ArrowLongMemoryAllocator( vector.getUInt8Vector() );
      case FLOAT:
        return new ArrowFloatMemoryAllocator( vector.getFloat4Vector() );
      case DOUBLE:
        return new ArrowDoubleMemoryAllocator( vector.getFloat8Vector() );
      case STRING:
        return new ArrowStringMemoryAllocator( vector.getVarCharVector() );
      case BYTES:
        return new ArrowBytesMemoryAllocator( vector.getVarBinaryVector() );

      case NULL:
      case EMPTY_ARRAY:
      case EMPTY_SPREAD:
      default:
        return ArrowNullMemoryAllocator.getInstance();
    }
  }

}
