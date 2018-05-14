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
package jp.co.yahoo.dataplatform.mds.hadoop.hive.util;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;


public final class HiveConfigurationUtil{


  public static String[] getColumnNames( final Configuration config ){
    String columnNames = config.get( IOConstants.COLUMNS );
    List<String> columnNameList = (List<String>) VirtualColumn.removeVirtualColumns( StringUtils.getStringCollection( columnNames ) );
    return columnNameList.toArray( new String[ columnNameList.size() ] );
  }

  public static TypeInfo[] getTypeInfos( final Configuration config ){
    String columnTypes = config.get(IOConstants.COLUMNS_TYPES);
    List<TypeInfo> typeInfoList = TypeInfoUtils.getTypeInfosFromTypeString( columnTypes );
    return typeInfoList.toArray( new TypeInfo[typeInfoList.size()] );
  }

}
