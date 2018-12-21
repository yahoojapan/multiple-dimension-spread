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

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;

public class TestMDSObjectInspectorFactory{

  @Test
  public void T_craeteObjectInspectorFromTypeInfo_1(){
    ObjectInspector oi = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( TypeInfoFactory.binaryTypeInfo );
    assertTrue( oi instanceof BinaryObjectInspector );
  }

  @Test
  public void T_craeteObjectInspectorFromTypeInfo_2(){
    ObjectInspector oi = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( TypeInfoFactory.booleanTypeInfo );
    assertTrue( oi instanceof BooleanObjectInspector );
  }

  @Test
  public void T_craeteObjectInspectorFromTypeInfo_3(){
    ObjectInspector oi = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( TypeInfoFactory.byteTypeInfo );
    assertTrue( oi instanceof ByteObjectInspector );
  }

  @Test
  public void T_craeteObjectInspectorFromTypeInfo_4(){
    assertThrows( UnsupportedOperationException.class ,
      () -> {
        ObjectInspector oi = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( TypeInfoFactory.dateTypeInfo );
      }
    );
  }

  @Test
  public void T_craeteObjectInspectorFromTypeInfo_5(){
    assertThrows( UnsupportedOperationException.class ,
      () -> {
        ObjectInspector oi = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( TypeInfoFactory.decimalTypeInfo );
      }
    );
  }

  @Test
  public void T_craeteObjectInspectorFromTypeInfo_6(){
    ObjectInspector oi = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( TypeInfoFactory.doubleTypeInfo );
    assertTrue( oi instanceof DoubleObjectInspector );
  }

  @Test
  public void T_craeteObjectInspectorFromTypeInfo_7(){
    ObjectInspector oi = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( TypeInfoFactory.floatTypeInfo );
    assertTrue( oi instanceof FloatObjectInspector );
  }

  @Test
  public void T_craeteObjectInspectorFromTypeInfo_8(){
    ObjectInspector oi = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( TypeInfoFactory.intTypeInfo );
    assertTrue( oi instanceof IntObjectInspector );
  }

  @Test
  public void T_craeteObjectInspectorFromTypeInfo_9(){
    ObjectInspector oi = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( TypeInfoFactory.longTypeInfo );
    assertTrue( oi instanceof LongObjectInspector );
  }

  @Test
  public void T_craeteObjectInspectorFromTypeInfo_10(){
    ObjectInspector oi = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( TypeInfoFactory.shortTypeInfo );
    assertTrue( oi instanceof ShortObjectInspector );
  }

  @Test
  public void T_craeteObjectInspectorFromTypeInfo_11(){
    ObjectInspector oi = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( TypeInfoFactory.stringTypeInfo );
    assertTrue( oi instanceof StringObjectInspector );
  }

  @Test
  public void T_craeteObjectInspectorFromTypeInfo_12(){
    assertThrows( UnsupportedOperationException.class ,
      () -> {
        ObjectInspector oi = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( TypeInfoFactory.timestampTypeInfo );
      }
    );
  }

  @Test
  public void T_craeteObjectInspectorFromTypeInfo_13(){
    assertThrows( UnsupportedOperationException.class ,
      () -> {
        ObjectInspector oi = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( TypeInfoFactory.unknownTypeInfo );
      }
    );
  }

  @Test
  public void T_craeteObjectInspectorFromTypeInfo_14(){
    assertThrows( UnsupportedOperationException.class ,
      () -> {
        ObjectInspector oi = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( TypeInfoFactory.voidTypeInfo );
      }
    );
  }

  @Test
  public void T_craeteObjectInspectorFromTypeInfo_15(){
    ListTypeInfo info = new ListTypeInfo();
    info.setListElementTypeInfo( TypeInfoFactory.stringTypeInfo );
    ObjectInspector oi = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( info );
    assertTrue( oi instanceof ListObjectInspector );
  }

  @Test
  public void T_craeteObjectInspectorFromTypeInfo_16(){
    MapTypeInfo info = new MapTypeInfo();
    info.setMapKeyTypeInfo( TypeInfoFactory.stringTypeInfo );
    info.setMapValueTypeInfo( TypeInfoFactory.stringTypeInfo );
    ObjectInspector oi = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( info );
    assertTrue( oi instanceof MapObjectInspector );
  }

  @Test
  public void T_craeteObjectInspectorFromTypeInfo_17(){
    StructTypeInfo info = new StructTypeInfo();
    ArrayList<String> fieldNameList = new ArrayList<String>();
    fieldNameList.add( "a" );
    fieldNameList.add( "b" );
    ArrayList<TypeInfo> typeInfoList = new ArrayList<TypeInfo>();
    typeInfoList.add( TypeInfoFactory.stringTypeInfo );
    typeInfoList.add( TypeInfoFactory.stringTypeInfo );

    info.setAllStructFieldNames( fieldNameList );
    info.setAllStructFieldTypeInfos( typeInfoList );
    ObjectInspector oi = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( info );
    assertTrue( oi instanceof StructObjectInspector );
  }

  @Test
  public void T_craeteObjectInspectorFromTypeInfo_18(){
    UnionTypeInfo unionTypeInfo = new UnionTypeInfo();
    List<TypeInfo> c = new ArrayList<TypeInfo>();
    c.add( TypeInfoFactory.stringTypeInfo );
    unionTypeInfo.setAllUnionObjectTypeInfos( c );
    ObjectInspector oi = MDSObjectInspectorFactory.craeteObjectInspectorFromTypeInfo( unionTypeInfo );
    assertTrue( oi instanceof UnionObjectInspector );
  }

}
