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
package jp.co.yahoo.dataplatform.mds.spread.analyzer;

import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

public final class ColumnAnalizerFactory{

  private ColumnAnalizerFactory(){}

  public static IColumnAnalizer get( final IColumn column ){
    switch( column.getColumnType() ){
      case UNION:
      case ARRAY:
      case SPREAD:
        return null;
      case BOOLEAN:
        return new BooleanColumnAnalizer( column );
      case BYTE:
        return new ByteColumnAnalizer( column );
      case DOUBLE:
        return new DoubleColumnAnalizer( column );
      case FLOAT:
        return new FloatColumnAnalizer( column );
      case INTEGER:
        return new IntegerColumnAnalizer( column );
      case LONG:
        return new LongColumnAnalizer( column );
      case SHORT:
        return new ShortColumnAnalizer( column );
      case BYTES:
        return new BytesColumnAnalizer( column );
      case STRING:
        return new StringColumnAnalizer( column );
      default:
        return null;
    }
  }

}
