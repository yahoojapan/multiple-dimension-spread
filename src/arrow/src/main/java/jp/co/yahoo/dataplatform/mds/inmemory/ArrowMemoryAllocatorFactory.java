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
import org.apache.arrow.vector.*;
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
        UnionVector unionVector = vector.addOrGetUnion( columnName );
        return new ArrowUnionMemoryAllocator( allocator , unionVector );
      case ARRAY:
        return new ArrowArrayMemoryAllocator( allocator , vector.addOrGetList( columnName ) );
      case SPREAD:
        MapVector mapVector = vector.addOrGetMap( columnName );
        return new ArrowMapMemoryAllocator( allocator , mapVector );

      case BOOLEAN:
        BitVector bitVector =  vector.addOrGet( columnName , new FieldType( true , ArrowType.Bool.INSTANCE , null , null ) , BitVector.class );
        return new ArrowBooleanMemoryAllocator( bitVector );
      case BYTE:
        TinyIntVector byteVector =  vector.addOrGet( columnName , new FieldType( true , new ArrowType.Int( 8 , true ) , null , null ) , TinyIntVector.class );
        return new ArrowByteMemoryAllocator( byteVector );
      case SHORT:
        SmallIntVector shortVector =  vector.addOrGet( columnName , new FieldType( true , new ArrowType.Int( 16 , true ) , null , null ) , SmallIntVector.class );
        return new ArrowShortMemoryAllocator( shortVector );
      case INTEGER:
        IntVector integerVector =  vector.addOrGet( columnName , new FieldType( true , new ArrowType.Int( 32 , true ) , null , null ) , IntVector.class );
        return new ArrowIntegerMemoryAllocator( integerVector );
      case LONG:
        BigIntVector longVector =  vector.addOrGet( columnName , new FieldType( true , new ArrowType.Int( 64 , true ) , null , null ) , BigIntVector.class );
        return new ArrowLongMemoryAllocator( longVector );
      case FLOAT:
        Float4Vector floatVector =  vector.addOrGet( columnName , new FieldType( true , new ArrowType.FloatingPoint( FloatingPointPrecision.SINGLE ) , null , null ) , Float4Vector.class );
        return new ArrowFloatMemoryAllocator( floatVector );
      case DOUBLE:
        Float8Vector doubleVector =  vector.addOrGet( columnName , new FieldType( true , new ArrowType.FloatingPoint( FloatingPointPrecision.DOUBLE ) , null , null ) , Float8Vector.class );
        return new ArrowDoubleMemoryAllocator( doubleVector );
      case STRING:
        VarCharVector charVector =  vector.addOrGet( columnName , new FieldType( true , ArrowType.Utf8.INSTANCE , null , null ) , VarCharVector.class );
        return new ArrowStringMemoryAllocator( charVector );
      case BYTES:
        VarBinaryVector binaryVector =  vector.addOrGet( columnName , new FieldType( true , ArrowType.Binary.INSTANCE , null , null ) , VarBinaryVector.class );
        return new ArrowBytesMemoryAllocator( binaryVector );

      case NULL:
      case EMPTY_ARRAY:
      case EMPTY_SPREAD:
      default:
        return NullMemoryAllocator.INSTANCE;
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
        AddOrGetResult<BitVector> bitVector =  vector.addOrGetVector( new FieldType( true , ArrowType.Bool.INSTANCE , null , null ) );
        return new ArrowBooleanMemoryAllocator( bitVector.getVector() );
      case BYTE:
        AddOrGetResult<TinyIntVector> byteVector =  vector.addOrGetVector( new FieldType( true , new ArrowType.Int( 8 , true ) , null , null ) );
        return new ArrowByteMemoryAllocator( byteVector.getVector() );
      case SHORT:
        AddOrGetResult<SmallIntVector> shortVector =  vector.addOrGetVector( new FieldType( true , new ArrowType.Int( 16 , true ) , null , null ) );
        return new ArrowShortMemoryAllocator( shortVector.getVector() );
      case INTEGER:
        AddOrGetResult<IntVector> integerVector =  vector.addOrGetVector( new FieldType( true , new ArrowType.Int( 32 , true ) , null , null ) );
        return new ArrowIntegerMemoryAllocator( integerVector.getVector() );
      case LONG:
        AddOrGetResult<BigIntVector> longVector =  vector.addOrGetVector( new FieldType( true , new ArrowType.Int( 64 , true ) , null , null ) );
        return new ArrowLongMemoryAllocator( longVector.getVector() );
      case FLOAT:
        AddOrGetResult<Float4Vector> floatVector =  vector.addOrGetVector( new FieldType( true , new ArrowType.FloatingPoint( FloatingPointPrecision.HALF ) , null , null ) );
        return new ArrowFloatMemoryAllocator( floatVector.getVector() );
      case DOUBLE:
        AddOrGetResult<Float8Vector> doubleVector =  vector.addOrGetVector( new FieldType( true , new ArrowType.FloatingPoint( FloatingPointPrecision.DOUBLE ) , null , null ) );
        return new ArrowDoubleMemoryAllocator( doubleVector.getVector() );
      case STRING:
        AddOrGetResult<VarCharVector> charVector =  vector.addOrGetVector( new FieldType( true , ArrowType.Utf8.INSTANCE , null , null ) );
        return new ArrowStringMemoryAllocator( charVector.getVector() );
      case BYTES:
        AddOrGetResult<VarBinaryVector> binaryVector =  vector.addOrGetVector( new FieldType( true , ArrowType.Binary.INSTANCE , null , null ) );
        return new ArrowBytesMemoryAllocator( binaryVector.getVector() );

      case NULL:
      case EMPTY_ARRAY:
      case EMPTY_SPREAD:
      default:
        return NullMemoryAllocator.INSTANCE;
    }
  }

  public static IMemoryAllocator getFromUnionVector( final ColumnType columnType , final String columnName , final BufferAllocator allocator , final UnionVector vector ){
    switch( columnType ){
      case UNION:
        return NullMemoryAllocator.INSTANCE;
      case ARRAY:
        return new ArrowArrayMemoryAllocator( allocator , vector.getList() );
      case SPREAD:
        return new ArrowMapMemoryAllocator( allocator , vector.getMap() );

      case BOOLEAN:
        return new ArrowBooleanMemoryAllocator( vector.getBitVector() );
      case BYTE:
        return new ArrowByteMemoryAllocator( vector.getTinyIntVector() );
      case SHORT:
        return new ArrowShortMemoryAllocator( vector.getSmallIntVector() );
      case INTEGER:
        return new ArrowIntegerMemoryAllocator( vector.getIntVector() );
      case LONG:
        return new ArrowLongMemoryAllocator( vector.getBigIntVector() );
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
        return NullMemoryAllocator.INSTANCE;
    }
  }

}
