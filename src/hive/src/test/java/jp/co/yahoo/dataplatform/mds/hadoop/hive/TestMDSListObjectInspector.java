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

import java.util.List;
import java.util.ArrayList;
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

public class TestMDSListObjectInspector{

  @Test
  public void T_newInstance_1(){
    ListTypeInfo info = new ListTypeInfo();
    info.setListElementTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSListObjectInspector inspector = new MDSListObjectInspector( info );
  }

  @Test
  public void T_newInstance_2(){
    ListTypeInfo childInfo = new ListTypeInfo();
    childInfo.setListElementTypeInfo( TypeInfoFactory.stringTypeInfo );

    ListTypeInfo info = new ListTypeInfo();
    info.setListElementTypeInfo( childInfo );

    MDSListObjectInspector inspector = new MDSListObjectInspector( info );
  }

  @Test
  public void T_getListElementObjectInspector_1(){
    ListTypeInfo info = new ListTypeInfo();
    info.setListElementTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSListObjectInspector inspector = new MDSListObjectInspector( info );

    assertTrue( inspector.getListElementObjectInspector() instanceof StringObjectInspector );
  }

  @Test
  public void T_getListElement_1() throws IOException{
    ListTypeInfo info = new ListTypeInfo();
    info.setListElementTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSListObjectInspector inspector = new MDSListObjectInspector( info );

    Map<String,Object> dataContainer = new HashMap<String,Object>();
    List<Object> array = new ArrayList<Object>();
    array.add( new StringObj( "a" ) );
    array.add( new StringObj( "b" ) );
    array.add( new StringObj( "c" ) );
    dataContainer.put( "array" , array );
    Spread spread = new Spread();
    spread.addRow( dataContainer );

    ColumnAndIndex columnAndIndex = new ColumnAndIndex( spread.getColumn( "array" ) , 0 , 0 );
    assertEquals( new Text( "a" ) , (Text)( inspector.getListElement( columnAndIndex , 0 ) ) );
    assertEquals( new Text( "b" ) , (Text)( inspector.getListElement( columnAndIndex , 1 ) ) );
    assertEquals( new Text( "c" ) , (Text)( inspector.getListElement( columnAndIndex , 2 ) ) );
  }

  @Test
  public void T_getListElement_2() throws IOException{
    ListTypeInfo childInfo = new ListTypeInfo();
    childInfo.setListElementTypeInfo( TypeInfoFactory.stringTypeInfo );

    ListTypeInfo info = new ListTypeInfo();
    info.setListElementTypeInfo( childInfo );
    MDSListObjectInspector inspector = new MDSListObjectInspector( info );
    MDSListObjectInspector childInspector = (MDSListObjectInspector)( inspector.getListElementObjectInspector() );

    Map<String,Object> dataContainer = new HashMap<String,Object>();
    List<Object> array = new ArrayList<Object>();
    List<Object> array2 = new ArrayList<Object>();
    array2.add( new StringObj( "a" ) );
    array2.add( new StringObj( "b" ) );
    array2.add( new StringObj( "c" ) );
    array.add( array2 );
    dataContainer.put( "array" , array );
    Spread spread = new Spread();
    spread.addRow( dataContainer );

    ColumnAndIndex columnAndIndex = new ColumnAndIndex( spread.getColumn( "array" ) , 0 , 0 );
    ColumnAndIndex childColumnAndIndex = (ColumnAndIndex)( inspector.getListElement( columnAndIndex , 0 ) ); 
    assertEquals( new Text( "a" ) , (Text)( childInspector.getListElement( childColumnAndIndex , 0 ) ) );
    assertEquals( new Text( "b" ) , (Text)( childInspector.getListElement( childColumnAndIndex , 1 ) ) );
    assertEquals( new Text( "c" ) , (Text)( childInspector.getListElement( childColumnAndIndex , 2 ) ) );
  }

  @Test
  public void T_getListLength_1() throws IOException{
    ListTypeInfo info = new ListTypeInfo();
    info.setListElementTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSListObjectInspector inspector = new MDSListObjectInspector( info );

    Map<String,Object> dataContainer = new HashMap<String,Object>();
    List<Object> array = new ArrayList<Object>();
    array.add( new StringObj( "a" ) );
    array.add( new StringObj( "b" ) );
    array.add( new StringObj( "c" ) );
    dataContainer.put( "array" , array );
    Spread spread = new Spread();
    spread.addRow( dataContainer );

    ColumnAndIndex columnAndIndex = new ColumnAndIndex( spread.getColumn( "array" ) , 0 , 0 );
    assertEquals( 3 , inspector.getListLength( columnAndIndex ) );
  }

  @Test
  public void T_getListLength_2() throws IOException{
    ListTypeInfo info = new ListTypeInfo();
    info.setListElementTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSListObjectInspector inspector = new MDSListObjectInspector( info );

    assertEquals( 0 , inspector.getListLength( null ) );
  }

  @Test
  public void T_getListLength_3() throws IOException{
    ListTypeInfo info = new ListTypeInfo();
    info.setListElementTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSListObjectInspector inspector = new MDSListObjectInspector( info );

    List<Object> target = new ArrayList<Object>();
    target.add(1);

    assertEquals( 1 , inspector.getListLength( target ) );
  }

  @Test
  public void T_getList_1() throws IOException{
    ListTypeInfo info = new ListTypeInfo();
    info.setListElementTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSListObjectInspector inspector = new MDSListObjectInspector( info );

    Map<String,Object> dataContainer = new HashMap<String,Object>();
    List<Object> array = new ArrayList<Object>();
    array.add( new StringObj( "a" ) );
    array.add( new StringObj( "b" ) );
    array.add( new StringObj( "c" ) );
    dataContainer.put( "array" , array );
    Spread spread = new Spread();
    spread.addRow( dataContainer );

    ColumnAndIndex columnAndIndex = new ColumnAndIndex( spread.getColumn( "array" ) , 0 , 0 );
    List list = inspector.getList( columnAndIndex );
    assertEquals( 3 , list.size() );
    assertEquals( new Text( "a" ) , (Text)( list.get(0) ) );
    assertEquals( new Text( "b" ) , (Text)( list.get(1) ) );
    assertEquals( new Text( "c" ) , (Text)( list.get(2) ) );
  }

  @Test
  public void T_getList_2() throws IOException{
    ListTypeInfo info = new ListTypeInfo();
    info.setListElementTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSListObjectInspector inspector = new MDSListObjectInspector( info );

    List<Object> target = new ArrayList<Object>();
    target.add(1);
    List list = inspector.getList( target );

    assertEquals( 1 , list.get(0) );
  }

  @Test
  public void T_getTypeName_1() throws IOException{
    ListTypeInfo info = new ListTypeInfo();
    info.setListElementTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSListObjectInspector inspector = new MDSListObjectInspector( info );

    assertEquals( "array<string>" , inspector.getTypeName() );
  }

  @Test
  public void T_getCategory_1() throws IOException{
    ListTypeInfo info = new ListTypeInfo();
    info.setListElementTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSListObjectInspector inspector = new MDSListObjectInspector( info );

    assertEquals( Category.LIST , inspector.getCategory() );
  }

  @Test
  public void T_create_1() throws IOException{
    ListTypeInfo info = new ListTypeInfo();
    info.setListElementTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSListObjectInspector inspector = new MDSListObjectInspector( info );

    List result = (List)( inspector.create( 5 ) );
    assertEquals( 5 , result.size() );
    assertEquals( null , result.get(0) );
    assertEquals( null , result.get(1) );
    assertEquals( null , result.get(2) );
    assertEquals( null , result.get(3) );
    assertEquals( null , result.get(4) );
  } 


  @Test
  public void T_resize_1() throws IOException{
    ListTypeInfo info = new ListTypeInfo();
    info.setListElementTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSListObjectInspector inspector = new MDSListObjectInspector( info );

    List result = (List)( inspector.create( 1 ) );
    assertEquals( 1 , result.size() );

    result = (List)( inspector.resize( result , 5 ) );
    assertEquals( 5 , result.size() );
    assertEquals( null , result.get(0) );
    assertEquals( null , result.get(1) );
    assertEquals( null , result.get(2) );
    assertEquals( null , result.get(3) );
    assertEquals( null , result.get(4) );
  }

  @Test
  public void T_resize_2() throws IOException{
    ListTypeInfo info = new ListTypeInfo();
    info.setListElementTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSListObjectInspector inspector = new MDSListObjectInspector( info );

    List result = (List)( inspector.create( 5 ) );
    assertEquals( 5 , result.size() );

    result = (List)( inspector.resize( result , 1 ) );
    assertEquals( 1 , result.size() );
    assertEquals( null , result.get(0) );
  }

  @Test
  public void T_set_1() throws IOException{
    ListTypeInfo info = new ListTypeInfo();
    info.setListElementTypeInfo( TypeInfoFactory.stringTypeInfo );
    MDSListObjectInspector inspector = new MDSListObjectInspector( info );

    List result = (List)( inspector.create( 5 ) );
    inspector.set( result , 3 , "hoge" );
    assertEquals( "hoge" , result.get( 3 ) );
  }

}
