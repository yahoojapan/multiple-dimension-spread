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

import jp.co.yahoo.dataplatform.mds.*;

public class TestColumnVectorAssignorFactory{

  private static TypeInfo createStruct(){
    List<String> name = new ArrayList<String>();
    name.add("hoge");
    List<TypeInfo> info = new ArrayList<TypeInfo>();
    info.add( TypeInfoFactory.intTypeInfo );

    return TypeInfoFactory.getStructTypeInfo( name , info );
  }

  private static TypeInfo createUnion(){
    List<TypeInfo> info = new ArrayList<TypeInfo>();
    info.add( TypeInfoFactory.intTypeInfo );

    return TypeInfoFactory.getUnionTypeInfo( info );
  }

  private static TypeInfo createArray(){
    return TypeInfoFactory.getListTypeInfo( TypeInfoFactory.intTypeInfo );
  }

  public static Stream<Arguments> data1() {
    return Stream.of(
      arguments( TypeInfoFactory.binaryTypeInfo , BytesColumnVectorAssignor.class.getName() ),
      arguments( TypeInfoFactory.stringTypeInfo , BytesColumnVectorAssignor.class.getName() )
    );
  }

  public static Stream<Arguments> data2() {
    return Stream.of(
      arguments( TypeInfoFactory.booleanTypeInfo , LongColumnVectorAssignor.class.getName() ),
      arguments( TypeInfoFactory.byteTypeInfo , LongColumnVectorAssignor.class.getName() ),
      arguments( TypeInfoFactory.shortTypeInfo , LongColumnVectorAssignor.class.getName() ),
      arguments( TypeInfoFactory.intTypeInfo , LongColumnVectorAssignor.class.getName() ),
      arguments( TypeInfoFactory.longTypeInfo , LongColumnVectorAssignor.class.getName() )
    );
  }

  public static Stream<Arguments> data3() {
    return Stream.of(
      arguments( TypeInfoFactory.floatTypeInfo , DoubleColumnVectorAssignor.class.getName() ),
      arguments( TypeInfoFactory.doubleTypeInfo , DoubleColumnVectorAssignor.class.getName() )
    );
  }

  public static Stream<Arguments> data4() {
    return Stream.of(
      arguments( TypeInfoFactory.charTypeInfo ),
      arguments( TypeInfoFactory.dateTypeInfo ),
      arguments( TypeInfoFactory.decimalTypeInfo ),
      arguments( TypeInfoFactory.timestampTypeInfo ),
      arguments( TypeInfoFactory.unknownTypeInfo ),
      arguments( TypeInfoFactory.varcharTypeInfo ),
      arguments( TypeInfoFactory.voidTypeInfo ),
      arguments( createStruct() ),
      arguments( createArray() ),
      arguments( createUnion() )
    );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_bytes_1( final TypeInfo typeInfo , final String resultClassName ){
    IColumnVectorAssignor assignor = ColumnVectorAssignorFactory.create( typeInfo );
    assertEquals( assignor.getClass().getName() , resultClassName );
  }

  @ParameterizedTest
  @MethodSource( "data2" )
  public void T_long_1( final TypeInfo typeInfo , final String resultClassName ){
    IColumnVectorAssignor assignor = ColumnVectorAssignorFactory.create( typeInfo );
    assertEquals( assignor.getClass().getName() , resultClassName );
  }

  @ParameterizedTest
  @MethodSource( "data3" )
  public void T_double_1( final TypeInfo typeInfo , final String resultClassName ){
    IColumnVectorAssignor assignor = ColumnVectorAssignorFactory.create( typeInfo );
    assertEquals( assignor.getClass().getName() , resultClassName );
  }

  @ParameterizedTest
  @MethodSource( "data4" )
  public void T_not_support_1( final TypeInfo typeInfo ){
    assertThrows( UnsupportedOperationException.class ,
      () -> {
        IColumnVectorAssignor assignor = ColumnVectorAssignorFactory.create( typeInfo );
      }
    );
  }

}
