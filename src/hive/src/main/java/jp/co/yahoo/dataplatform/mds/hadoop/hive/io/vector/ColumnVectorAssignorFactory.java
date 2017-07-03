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
package jp.co.yahoo.dataplatform.mds.hadoop.hive.io.vector;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

public final class ColumnVectorAssignorFactory{

  private ColumnVectorAssignorFactory(){}

  public static IColumnVectorAssignor create( final TypeInfo typeInfo ){
    switch ( typeInfo.getCategory() ){
      case PRIMITIVE:
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo)typeInfo;
        switch( primitiveTypeInfo.getPrimitiveCategory() ){
          case STRING:
          case BINARY:
            return new BytesColumnVectorAssignor();
          case BYTE:
            return new LongColumnVectorAssignor( BytePrimitiveSetter.getInstance() );
          case SHORT:
            return new LongColumnVectorAssignor( ShortPrimitiveSetter.getInstance() );
          case INT:
            return new LongColumnVectorAssignor( IntegerPrimitiveSetter.getInstance() );
          case BOOLEAN:
          case LONG:
            return new LongColumnVectorAssignor( LongPrimitiveSetter.getInstance() );
          case FLOAT:
            return new DoubleColumnVectorAssignor( FloatPrimitiveSetter.getInstance() );
          case DOUBLE:
            return new DoubleColumnVectorAssignor( DoublePrimitiveSetter.getInstance() );
          case DATE:
          case DECIMAL:
          case TIMESTAMP:
          case VOID:
          default:
            throw new UnsupportedOperationException( "Unsupport vectorize column " + primitiveTypeInfo.getPrimitiveCategory() );
        }
      case STRUCT:
      case MAP:
      case LIST:
      case UNION:
      default:
        throw new UnsupportedOperationException( "Unsupport vectorize column " + typeInfo.getCategory() );
    }
  }


}
