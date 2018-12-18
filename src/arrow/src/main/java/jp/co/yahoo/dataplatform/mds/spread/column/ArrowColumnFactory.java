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
package jp.co.yahoo.dataplatform.mds.spread.column;

import java.util.Map;
import java.util.HashMap;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.ListVector;

public final class ArrowColumnFactory{

  private static final Map<Class, ColumnFactory> dispatch = new HashMap<>();

  static{
    dispatch.put( BitVector.class , ( name , v ) ->  new ArrowPrimitiveColumn( new ArrowBooleanConnector( name , (BitVector)v ) ) );
    dispatch.put( TinyIntVector.class , ( name , v ) -> new ArrowPrimitiveColumn( new ArrowByteConnector( name , (TinyIntVector)v ) ) );
    dispatch.put( SmallIntVector.class , ( name , v ) -> new ArrowPrimitiveColumn( new ArrowShortConnector( name , (SmallIntVector)v ) ) );
    dispatch.put( IntVector.class , ( name , v ) -> new ArrowPrimitiveColumn( new ArrowIntegerConnector( name , (IntVector)v ) ) );
    dispatch.put( BigIntVector.class , ( name , v ) -> new ArrowPrimitiveColumn( new ArrowLongConnector( name , (BigIntVector)v ) ) );
    dispatch.put( Float4Vector.class , ( name , v ) -> new ArrowPrimitiveColumn( new ArrowFloatConnector( name , (Float4Vector)v ) ) );
    dispatch.put( Float8Vector.class , ( name , v ) -> new ArrowPrimitiveColumn( new ArrowDoubleConnector( name , (Float8Vector)v ) ) );
    dispatch.put( VarCharVector.class , ( name , v ) -> new ArrowPrimitiveColumn( new ArrowStringConnector( name , (VarCharVector)v ) ) );
    dispatch.put( VarBinaryVector.class , ( name , v ) -> new ArrowPrimitiveColumn( new ArrowBytesConnector( name , (VarBinaryVector)v ) ) );
    dispatch.put( StructVector.class , ( name , v ) -> new ArrowStructColumn( name , (StructVector)v ) );
    dispatch.put( ListVector.class , ( name , v ) -> new ArrowArrayColumn( name , (ListVector)v ) );
  }

  public static IColumn convert( final String name , final ValueVector vector ){
    ColumnFactory factory = dispatch.get( vector.getClass() );
    if( factory == null ){
      throw new UnsupportedOperationException( "Unsupported vector : " + vector.getClass().getName() );
    }
    return factory.get( name , vector );
  }

  @FunctionalInterface
  private static interface ColumnFactory {
    IColumn get( final String columnName , final ValueVector vector ) throws UnsupportedOperationException;
  }

}
