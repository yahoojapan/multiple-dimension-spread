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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import jp.co.yahoo.dataplatform.mds.hadoop.hive.io.ColumnAndIndex;
import jp.co.yahoo.dataplatform.mds.spread.Spread;
import org.testng.annotations.Test;

import org.apache.hadoop.io.*;

import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

import jp.co.yahoo.dataplatform.schema.objects.*;

public class TestMDSMapObjectInspector{

  @Test
  public void T_newInstance_1(){
    MapTypeInfo info = new MapTypeInfo();
    info.setMapKeyTypeInfo( TypeInfoFactory.stringTypeInfo );
    info.setMapValueTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSMapObjectInspector inspector = new MDSMapObjectInspector( info );
  }

  @Test( expectedExceptions = { RuntimeException.class } )
  public void T_newInstance_2(){
    MapTypeInfo info = new MapTypeInfo();
    info.setMapKeyTypeInfo( TypeInfoFactory.longTypeInfo );
    info.setMapValueTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSMapObjectInspector inspector = new MDSMapObjectInspector( info );
  }

  @Test
  public void T_getMapKeyObjectInspector_1(){
    MapTypeInfo info = new MapTypeInfo();
    info.setMapKeyTypeInfo( TypeInfoFactory.stringTypeInfo );
    info.setMapValueTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSMapObjectInspector inspector = new MDSMapObjectInspector( info );
    assertTrue( inspector.getMapKeyObjectInspector() instanceof StringObjectInspector );
  }

  @Test
  public void T_getMapValueObjectInspector_1(){
    MapTypeInfo info = new MapTypeInfo();
    info.setMapKeyTypeInfo( TypeInfoFactory.stringTypeInfo );
    info.setMapValueTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSMapObjectInspector inspector = new MDSMapObjectInspector( info );
    assertTrue( inspector.getMapValueObjectInspector() instanceof StringObjectInspector );
  }

  @Test
  public void T_getCategory_1(){
    MapTypeInfo info = new MapTypeInfo();
    info.setMapKeyTypeInfo( TypeInfoFactory.stringTypeInfo );
    info.setMapValueTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSMapObjectInspector inspector = new MDSMapObjectInspector( info );
    assertEquals( Category.MAP , inspector.getCategory() );
  }

  @Test
  public void T_getMapValueElement_1() throws IOException{
    Map<String,Object> dataContainer = new HashMap<String,Object>();
    Map<String,Object> map = new HashMap<String,Object>();
    map.put( "key1" , new StringObj( "aaa" ) );
    map.put( "key2" , new StringObj( "ccc" ) );
    map.put( "key3" , new IntegerObj( 100 ) );
    dataContainer.put( "map" , map );

    Spread spread = new Spread();
    spread.addRow( dataContainer );

    ColumnAndIndex columnAndIndex = new ColumnAndIndex( spread.getColumn( "map" ) , 0 , 0 );

    MapTypeInfo info = new MapTypeInfo();
    info.setMapKeyTypeInfo( TypeInfoFactory.stringTypeInfo );
    info.setMapValueTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSMapObjectInspector inspector = new MDSMapObjectInspector( info );

    Map<?,?> result = inspector.getMap( columnAndIndex );
    assertEquals( new Text( "aaa" ) , inspector.getMapValueElement( result , "key1" ) );
    assertEquals( new Text( "ccc" ) , inspector.getMapValueElement( result , "key2" ) );
    assertEquals( new Text( "100" ) , inspector.getMapValueElement( result , "key3" ) );
  }

  @Test
  public void T_getMap_1() throws IOException{
    Map<String,Object> dataContainer = new HashMap<String,Object>();
    Map<String,Object> map = new HashMap<String,Object>();
    map.put( "key1" , new StringObj( "aaa" ) );
    map.put( "key2" , new StringObj( "ccc" ) );
    map.put( "key3" , new IntegerObj( 100 ) );
    dataContainer.put( "map" , map );

    Spread spread = new Spread();
    spread.addRow( dataContainer );

    ColumnAndIndex columnAndIndex = new ColumnAndIndex( spread.getColumn( "map" ) , 0 , 0 );

    MapTypeInfo info = new MapTypeInfo();
    info.setMapKeyTypeInfo( TypeInfoFactory.stringTypeInfo );
    info.setMapValueTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSMapObjectInspector inspector = new MDSMapObjectInspector( info );

    Map<?,?> result = inspector.getMap( columnAndIndex );
    assertEquals( new Text( "aaa" ) , result.get( "key1" ) );
    assertEquals( new Text( "ccc" ) , result.get( "key2" ) );
    assertEquals( new Text( "100" ) , result.get( "key3" ) );
    assertEquals( 3 , result.size() );
  }

  @Test
  public void T_getMapSize_1() throws IOException{
    Map<String,Object> dataContainer = new HashMap<String,Object>();
    Map<String,Object> map = new HashMap<String,Object>();
    map.put( "key1" , new StringObj( "aaa" ) );
    map.put( "key2" , new StringObj( "ccc" ) );
    map.put( "key3" , new IntegerObj( 100 ) );
    dataContainer.put( "map" , map );

    Spread spread = new Spread();
    spread.addRow( dataContainer );

    ColumnAndIndex columnAndIndex = new ColumnAndIndex( spread.getColumn( "map" ) , 0 , 0 );

    MapTypeInfo info = new MapTypeInfo();
    info.setMapKeyTypeInfo( TypeInfoFactory.stringTypeInfo );
    info.setMapValueTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSMapObjectInspector inspector = new MDSMapObjectInspector( info );

    Map<?,?> result = inspector.getMap( columnAndIndex );
    assertEquals( 3 , inspector.getMapSize( result ) );
  }

}
