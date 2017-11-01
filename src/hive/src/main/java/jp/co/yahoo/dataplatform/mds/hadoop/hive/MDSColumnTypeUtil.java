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

import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

public final class MDSColumnTypeUtil {

  private MDSColumnTypeUtil(){}

  public static ColumnType typeInfoToColumnType( final TypeInfo typeInfo ){
    switch ( typeInfo.getCategory() ){
      case STRUCT:
        return ColumnType.SPREAD;
      case MAP:
        return ColumnType.SPREAD;
      case LIST:
        return ColumnType.ARRAY;
      case UNION:
        return ColumnType.UNION;
      case PRIMITIVE:
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo)typeInfo;
        switch( primitiveTypeInfo.getPrimitiveCategory() ){
          case STRING:
            return ColumnType.STRING;
          case BINARY:
            return ColumnType.BYTES;
          case BOOLEAN:
            return ColumnType.BOOLEAN;
          case BYTE:
            return ColumnType.BYTE;
          case DOUBLE:
            return ColumnType.DOUBLE;
          case FLOAT:
            return ColumnType.FLOAT;
          case INT:
            return ColumnType.INTEGER;
          case LONG:
            return ColumnType.LONG;
          case SHORT:
            return ColumnType.SHORT;

          case DATE:
          case DECIMAL:
          case TIMESTAMP:
          case VOID:
          default:
            return ColumnType.UNKNOWN;
        }
      default:
        return ColumnType.UNKNOWN;
    }
  }

}
