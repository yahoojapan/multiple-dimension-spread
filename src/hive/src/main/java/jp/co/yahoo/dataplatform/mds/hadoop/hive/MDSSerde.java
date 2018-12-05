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

import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

import jp.co.yahoo.dataplatform.mds.hadoop.hive.io.ParserWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;

import jp.co.yahoo.dataplatform.schema.parser.HiveStructParser;
import jp.co.yahoo.dataplatform.schema.parser.HiveMessageReader;

public class MDSSerde extends AbstractSerDe {

  private static final Logger LOG = LoggerFactory.getLogger(MDSSerde.class);

  private final HiveMessageReader messageReader = new HiveMessageReader();
  private final Map<String,Integer> filedIndexMap = new HashMap<String,Integer>();

  private HiveStructParser parser;
  private ObjectInspector inspector;

  private StructTypeInfo getAllReadTypeInfo( final String columnNameProperty , final String columnTypeProperty ){
    ArrayList<TypeInfo> fieldTypes = TypeInfoUtils.getTypeInfosFromTypeString( columnTypeProperty );
    ArrayList<String> columnNames = new ArrayList<String>();
    if ( columnNameProperty != null && 0 < columnNameProperty.length() ) {
      String[] columnNameArray = columnNameProperty.split(",");
      for( int i = 0 ; i < columnNameArray.length ; i++ ){
        columnNames.add( columnNameArray[i] );
        filedIndexMap.put( columnNameArray[i] , i );
      }
    }
    StructTypeInfo rootType = new StructTypeInfo();

    rootType.setAllStructFieldNames( columnNames );
    rootType.setAllStructFieldTypeInfos( fieldTypes );

    return rootType;
  }

  private StructTypeInfo getColumnProjectionTypeInfo( final String columnNameProperty , final String columnTypeProperty , final String projectionColumnNames ){
    Set<String> columnNameSet = new HashSet<String>();
    for( String columnName : projectionColumnNames.split(",") ){
      columnNameSet.add( columnName );
    }

    ArrayList<TypeInfo> fieldTypes = TypeInfoUtils.getTypeInfosFromTypeString( columnTypeProperty );
    String[] splitNames = columnNameProperty.split(",");

    ArrayList<String> projectionColumnNameList = new ArrayList<String>();
    ArrayList<TypeInfo> projectionFieldTypeList = new ArrayList<TypeInfo>();
    for( int i = 0 ; i < fieldTypes.size() ; i++ ){
      if( columnNameSet.contains( splitNames[i] ) ){
        projectionColumnNameList.add( splitNames[i] );
        projectionFieldTypeList.add( fieldTypes.get(i) );
      }
      filedIndexMap.put( splitNames[i] , i );
    }
    StructTypeInfo rootType = new StructTypeInfo();

    rootType.setAllStructFieldNames( projectionColumnNameList );
    rootType.setAllStructFieldTypeInfos( projectionFieldTypeList );

    return rootType;
  }

  @Override
  public void initialize( final Configuration conf, final Properties table ) throws SerDeException{
    initialize( conf , table , table );
  }

  @Override
  public void initialize( final Configuration conf, final Properties table , final Properties part ) throws SerDeException{
    LOG.info( table.toString() );
    if( part != null ){
      LOG.info( part.toString() );
    }
    String columnNameProperty = table.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = table.getProperty(serdeConstants.LIST_COLUMN_TYPES);

    String projectionColumnNames = conf.get( ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR , "" );

    StructTypeInfo rootType;
    if( projectionColumnNames.isEmpty() ){
      rootType = getAllReadTypeInfo( columnNameProperty , columnTypeProperty );
    }
    else{
      rootType = getColumnProjectionTypeInfo( columnNameProperty , columnTypeProperty , projectionColumnNames );
    }

    inspector = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( rootType );
  }

  @Override
  public Object deserialize( final Writable writable ) throws SerDeException {
    return writable;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException{
    return inspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass(){
    return ParserWritable.class;
  }

  @Override
  public Writable serialize( final Object obj, final ObjectInspector objInspector ) throws SerDeException{
    ParserWritable parserWritable = new ParserWritable();
    try{
      if( parser == null ){
        parser = (HiveStructParser)( messageReader.create( objInspector ) );
        parser.setFieldIndexMap( filedIndexMap );
      }
      parser.setObject( obj );
      parserWritable.set( parser );
    }catch( IOException e ){
      throw new SerDeException( e );
    }
    return parserWritable;
  }

  @Override
  public SerDeStats getSerDeStats(){
    return null;
  }

}
