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

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.dataplatform.mds.spread.column.SpreadColumn;

import org.apache.hadoop.io.*;

import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;

import jp.co.yahoo.dataplatform.schema.objects.*;

import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.hadoop.hive.io.ColumnAndIndex;

public class TestMDSStructObjectInspector{

  private StructTypeInfo getTypeInfo(){
    StructTypeInfo info = new StructTypeInfo();
    ArrayList<String> nameList = new ArrayList<String>();
    nameList.add( "str" );
    nameList.add( "num" );
    nameList.add( "arr" );
    nameList.add( "uni" );

    ArrayList<TypeInfo> typeInfoList = new ArrayList<TypeInfo>();
    typeInfoList.add( TypeInfoFactory.stringTypeInfo );
    typeInfoList.add( TypeInfoFactory.intTypeInfo );

    ListTypeInfo arrayInfo = new ListTypeInfo();
    arrayInfo.setListElementTypeInfo( TypeInfoFactory.stringTypeInfo );
    typeInfoList.add( arrayInfo );

    UnionTypeInfo unionTypeInfo = new UnionTypeInfo();
    ArrayList<TypeInfo> uniTypeInfoList = new ArrayList<TypeInfo>();
    uniTypeInfoList.add( TypeInfoFactory.stringTypeInfo );
    uniTypeInfoList.add( TypeInfoFactory.intTypeInfo );
    unionTypeInfo.setAllUnionObjectTypeInfos( uniTypeInfoList );
    typeInfoList.add( unionTypeInfo );

    info.setAllStructFieldNames( nameList );
    info.setAllStructFieldTypeInfos( typeInfoList );
    return info;
  }

  @Test
  public void T_newInstance_1(){
    MDSStructObjectInspector inspecote = new MDSStructObjectInspector( getTypeInfo() );
  }

  @Test
  public void T_getAllStructFieldRefs_1(){
    MDSStructObjectInspector inspector = new MDSStructObjectInspector( getTypeInfo() );
    List<StructField> fieldList = inspector.getAllStructFieldRefs();
    assertEquals( fieldList.get( 0 ).getFieldName() , "str" );
    assertEquals( fieldList.get( 1 ).getFieldName() , "num" );
    assertEquals( fieldList.get( 2 ).getFieldName() , "arr" );
    assertEquals( fieldList.get( 3 ).getFieldName() , "uni" );

    assertEquals( fieldList.get( 0 ).getFieldID() , 0 );
    assertEquals( fieldList.get( 1 ).getFieldID() , 1 );
    assertEquals( fieldList.get( 2 ).getFieldID() , 2 );
    assertEquals( fieldList.get( 3 ).getFieldID() , 3 );

    assertEquals( fieldList.get( 0 ).getFieldComment() , null );
    assertEquals( fieldList.get( 1 ).getFieldComment() , null );
    assertEquals( fieldList.get( 2 ).getFieldComment() , null );
    assertEquals( fieldList.get( 3 ).getFieldComment() , null );

    assertTrue( fieldList.get( 0 ).getFieldObjectInspector() instanceof StringObjectInspector );
    assertTrue( fieldList.get( 1 ).getFieldObjectInspector() instanceof IntObjectInspector );
    assertTrue( fieldList.get( 2 ).getFieldObjectInspector() instanceof ListObjectInspector );
    assertTrue( fieldList.get( 3 ).getFieldObjectInspector() instanceof UnionObjectInspector );
  }

  @Test
  public void T_getStructFieldRef_1(){
    MDSStructObjectInspector inspector = new MDSStructObjectInspector( getTypeInfo() );
    assertEquals( inspector.getStructFieldRef( "uni" ).getFieldName() , "uni" );

    assertEquals( inspector.getStructFieldRef( "uni" ).getFieldID() , 3 );

    assertEquals( inspector.getStructFieldRef( "uni" ).getFieldComment() , null );

    assertTrue( inspector.getStructFieldRef( "uni" ).getFieldObjectInspector() instanceof UnionObjectInspector );
  }

  @Test
  public void T_getTypeName_1(){
    MDSStructObjectInspector inspector = new MDSStructObjectInspector( getTypeInfo() );
    assertEquals( inspector.getTypeName() , "struct<str:string,num:int,arr:array<string>,uni:uniontype<string,int>>" );
  }

  @Test
  public void T_getCategory_1(){
    MDSStructObjectInspector inspector = new MDSStructObjectInspector( getTypeInfo() );
    assertEquals( inspector.getCategory() , Category.STRUCT );
  }

  @Test
  public void T_create_1(){
    MDSStructObjectInspector inspector = new MDSStructObjectInspector( getTypeInfo() );
    assertEquals( ( (List<Object>)( inspector.create() ) ).size() , 4 );
  }

  @Test
  public void T_equals_1(){
    MDSStructObjectInspector inspector = new MDSStructObjectInspector( getTypeInfo() );
    assertTrue( inspector.equals( inspector ) );
  }

  @Test
  public void T_setStructFieldData_1(){
    MDSStructObjectInspector inspector = new MDSStructObjectInspector( getTypeInfo() );
    Object hoge = inspector.setStructFieldData( inspector.create() , inspector.getStructFieldRef( "uni" ) , "hoge" );
  }

  @Test
  public void T_getStructFieldsDataAsList_1() throws IOException{
    MDSStructObjectInspector inspector = new MDSStructObjectInspector( getTypeInfo() );

    Map<String,Object> dataContainer = new HashMap<String,Object>();
    dataContainer.put( "str" , new StringObj( "a" ) );
    dataContainer.put( "num" , new IntegerObj( 1 ) );
    dataContainer.put( "uni" , new IntegerObj( 1 ) );

    Spread spread = new Spread();
    spread.addRow( dataContainer );
    SpreadColumn spreadColumn = new SpreadColumn( "root" );
    spreadColumn.setSpread( spread );
    ColumnAndIndex columnAndIndex = new ColumnAndIndex( spreadColumn , 0 , 0 );

    List<Object> result = inspector.getStructFieldsDataAsList( columnAndIndex );
    assertEquals( result.get(0) , new Text( "a" ) );
    assertEquals( result.get(1) , new IntWritable( 1 ) );
    assertTrue( result.get(2) instanceof ColumnAndIndex );
    assertTrue( result.get(3) instanceof StandardUnion );
  }

  @Test
  public void T_getStructFieldData_1() throws IOException{
    MDSStructObjectInspector inspector = new MDSStructObjectInspector( getTypeInfo() );

    Map<String,Object> dataContainer = new HashMap<String,Object>();
    dataContainer.put( "str" , new StringObj( "a" ) );
    dataContainer.put( "num" , new IntegerObj( 1 ) );
    dataContainer.put( "uni" , new IntegerObj( 1 ) );

    Spread spread = new Spread();
    spread.addRow( dataContainer );
    SpreadColumn spreadColumn = new SpreadColumn( "root" );
    spreadColumn.setSpread( spread );
    ColumnAndIndex columnAndIndex = new ColumnAndIndex( spreadColumn , 0 , 0 );

    Object result = inspector.getStructFieldData( columnAndIndex , inspector.getStructFieldRef( "str" ) );
    assertEquals( result , new Text( "a" ) );
  }

  @Test
  public void T_getStructFieldData_2() throws IOException{
    MDSStructObjectInspector inspector = new MDSStructObjectInspector( getTypeInfo() );
    List<Object> in = new ArrayList<Object>();
    in.add( new Text( "a" ) );
    Object result = inspector.getStructFieldData( in , inspector.getStructFieldRef( "str" ) );
    assertEquals( result , new Text( "a" ) );
  }

}
