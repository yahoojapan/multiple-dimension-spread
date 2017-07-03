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

import java.util.List;
import java.util.ArrayList;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;
import org.testng.annotations.DataProvider;

import org.apache.hadoop.hive.serde2.typeinfo.*;

import jp.co.yahoo.dataplatform.mds.*;

public class TestColumnVectorAssignorFactory{

  private TypeInfo createStruct(){
    List<String> name = new ArrayList<String>();
    name.add("hoge");
    List<TypeInfo> info = new ArrayList<TypeInfo>();
    info.add( TypeInfoFactory.intTypeInfo );

    return TypeInfoFactory.getStructTypeInfo( name , info );
  }

  private TypeInfo createUnion(){
    List<TypeInfo> info = new ArrayList<TypeInfo>();
    info.add( TypeInfoFactory.intTypeInfo );

    return TypeInfoFactory.getUnionTypeInfo( info );
  }

  private TypeInfo createArray(){
    return TypeInfoFactory.getListTypeInfo( TypeInfoFactory.intTypeInfo );
  }

  @DataProvider(name = "T_bytes_1")
  public Object[][] data() {
    return new Object[][] {
      { TypeInfoFactory.binaryTypeInfo , BytesColumnVectorAssignor.class.getName() },
      { TypeInfoFactory.stringTypeInfo , BytesColumnVectorAssignor.class.getName() },
    };
  }

  @DataProvider(name = "T_long_1")
  public Object[][] data2() {
    return new Object[][] {
      { TypeInfoFactory.booleanTypeInfo , LongColumnVectorAssignor.class.getName() },
      { TypeInfoFactory.byteTypeInfo , LongColumnVectorAssignor.class.getName() },
      { TypeInfoFactory.shortTypeInfo , LongColumnVectorAssignor.class.getName() },
      { TypeInfoFactory.intTypeInfo , LongColumnVectorAssignor.class.getName() },
      { TypeInfoFactory.longTypeInfo , LongColumnVectorAssignor.class.getName() },
    };
  }

  @DataProvider(name = "T_double_1")
  public Object[][] data3() {
    return new Object[][] {
      { TypeInfoFactory.floatTypeInfo , DoubleColumnVectorAssignor.class.getName() },
      { TypeInfoFactory.doubleTypeInfo , DoubleColumnVectorAssignor.class.getName() },
    };
  }

  @DataProvider(name = "T_not_support_1")
  public Object[][] data4() {
    return new Object[][] {
      { TypeInfoFactory.charTypeInfo },
      { TypeInfoFactory.dateTypeInfo },
      { TypeInfoFactory.decimalTypeInfo },
      { TypeInfoFactory.timestampTypeInfo },
      { TypeInfoFactory.unknownTypeInfo },
      { TypeInfoFactory.varcharTypeInfo },
      { TypeInfoFactory.voidTypeInfo },
      { createStruct() },
      { createArray() },
      { createUnion() },
    };
  }

  @Test( dataProvider = "T_bytes_1")
  public void T_bytes_1( final TypeInfo typeInfo , final String resultClassName ){
    IColumnVectorAssignor assignor = ColumnVectorAssignorFactory.create( typeInfo );
    assertEquals( assignor.getClass().getName() , resultClassName );
  }

  @Test( dataProvider = "T_long_1")
  public void T_long_1( final TypeInfo typeInfo , final String resultClassName ){
    IColumnVectorAssignor assignor = ColumnVectorAssignorFactory.create( typeInfo );
    assertEquals( assignor.getClass().getName() , resultClassName );
  }

  @Test( dataProvider = "T_double_1")
  public void T_double_1( final TypeInfo typeInfo , final String resultClassName ){
    IColumnVectorAssignor assignor = ColumnVectorAssignorFactory.create( typeInfo );
    assertEquals( assignor.getClass().getName() , resultClassName );
  }

  @Test( expectedExceptions = { UnsupportedOperationException.class } , dataProvider = "T_not_support_1" )
  public void T_not_support_1( final TypeInfo typeInfo ){
    IColumnVectorAssignor assignor = ColumnVectorAssignorFactory.create( typeInfo );
  }

}
