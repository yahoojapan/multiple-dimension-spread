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
package jp.co.yahoo.dataplatform.mds.hadoop.hive.io;

import java.util.List;
import java.util.ArrayList;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;

import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.hive.ql.exec.vector.*;

import jp.co.yahoo.dataplatform.mds.*;

public class TestHiveVectorizedReaderSetting{

  @Test
  public void T_createNeedColumnId_1(){
    List<Integer> intList = new ArrayList<Integer>();
    intList.add( 1 );
    intList.add( 2 );
    intList.add( 3 );
    intList.add( 4 );

    HiveVectorizedReaderSetting setting = new HiveVectorizedReaderSetting( null , null , null , null , null , null , new HiveReaderSetting( null , null , false , false , false ) );
    int[] intArray = setting.createNeedColumnId( intList );
    assertEquals( 1 , intArray[0] );
    assertEquals( 2 , intArray[1] );
    assertEquals( 3 , intArray[2] );
    assertEquals( 4 , intArray[3] );
  }

  @Test
  public void T_isVectorMode_1(){
    HiveVectorizedReaderSetting setting = new HiveVectorizedReaderSetting( null , null , null , null , null , null , new HiveReaderSetting( null , null , false , false , false ) );
    assertFalse( setting.isVectorMode() );
  }

  @Test
  public void T_getReaderConfig_1(){
    HiveVectorizedReaderSetting setting = new HiveVectorizedReaderSetting( null , null , null , null , null , null , new HiveReaderSetting( null , null , false , false , false ) );
    assertEquals( null , setting.getReaderConfig() );
  }

  @Test
  public void T_getExpressionNode_1(){
    HiveVectorizedReaderSetting setting = new HiveVectorizedReaderSetting( null , null , null , null , null , null , new HiveReaderSetting( null , null , false , false , false ) );
    assertEquals( null , setting.getExpressionNode() );
  }

  private VectorizedRowBatchCtx getVectorizedRowBatchCtx(){
    StructTypeInfo info = new StructTypeInfo();
    ArrayList<String> nameList = new ArrayList<String>();
    nameList.add( "str" );
    nameList.add( "num" );
    nameList.add( "arr" );
    nameList.add( "uni" );
    nameList.add( "p" );

    ArrayList<TypeInfo> typeInfoList = new ArrayList<TypeInfo>();
    typeInfoList.add( TypeInfoFactory.stringTypeInfo );
    typeInfoList.add( TypeInfoFactory.intTypeInfo );
    typeInfoList.add( TypeInfoFactory.stringTypeInfo );
    typeInfoList.add( TypeInfoFactory.stringTypeInfo );
    typeInfoList.add( TypeInfoFactory.intTypeInfo );

    info.setAllStructFieldNames( nameList );
    info.setAllStructFieldTypeInfos( typeInfoList );

    VectorizedRowBatchCtx rbCtx = new VectorizedRowBatchCtx( nameList.toArray( new String[0] ) , typeInfoList.toArray( new TypeInfo[0] ) , 1 , new String[0] );
    return rbCtx;
  }

  @Test
  public void T_createVectorizedRowBatch_1(){
    VectorizedRowBatchCtx rbCtx = getVectorizedRowBatchCtx();
    HiveVectorizedReaderSetting setting = new HiveVectorizedReaderSetting( new boolean[]{ true , true , true , true } , null , rbCtx , null , null , null , new HiveReaderSetting( null , null , false , false , false ) );
    VectorizedRowBatch batch = setting.createVectorizedRowBatch();
    assertEquals( batch.numCols , 5 );
    assertEquals( batch.cols[0].getClass().getName() , BytesColumnVector.class.getName() );
    assertEquals( batch.cols[1].getClass().getName() , LongColumnVector.class.getName() );
    assertEquals( batch.cols[2].getClass().getName() , BytesColumnVector.class.getName() );
    assertEquals( batch.cols[3].getClass().getName() , BytesColumnVector.class.getName() );
    assertEquals( batch.cols[4].getClass().getName() , LongColumnVector.class.getName() );
  }

  @Test
  public void T_setPartitionValues_1(){
    VectorizedRowBatchCtx rbCtx = getVectorizedRowBatchCtx();
    Object[] partitionValue = new Object[]{ Integer.valueOf(100) };
    HiveVectorizedReaderSetting setting = new HiveVectorizedReaderSetting( new boolean[]{ true , true , true , true , true } , partitionValue , rbCtx , null , null , null , new HiveReaderSetting( null , null , false , false , false ) );
    VectorizedRowBatch batch = setting.createVectorizedRowBatch();
    setting.setPartitionValues( batch );
    assertEquals( batch.numCols , 5 );
    batch.size = 10;
    System.out.println( batch.toString() );
    LongColumnVector vector = (LongColumnVector)batch.cols[4];
    assertEquals( vector.vector[0] , 100 );
  }

  @Test
  public void T_setPartitionValues_2(){
    VectorizedRowBatchCtx rbCtx = getVectorizedRowBatchCtx();
    Object[] partitionValue = new Object[0];
    HiveVectorizedReaderSetting setting = new HiveVectorizedReaderSetting( new boolean[]{ true , true , true , true } , partitionValue , rbCtx , null , null , null , new HiveReaderSetting( null , null , false , false , false ) );
    VectorizedRowBatch batch = setting.createVectorizedRowBatch();
    setting.setPartitionValues( batch );
    LongColumnVector vector = (LongColumnVector)batch.cols[4];
    assertEquals( vector.vector[0] , 0 );
  }

  @Test
  public void T_getAssignors_1(){
    HiveVectorizedReaderSetting setting = new HiveVectorizedReaderSetting( null , null , null , null , null , null , new HiveReaderSetting( null , null , false , false , false ) );
    assertEquals( setting.getAssignors() , null );
  }

  @Test
  public void T_getNeedColumnIds_1(){
    HiveVectorizedReaderSetting setting = new HiveVectorizedReaderSetting( null , null , null , null , null , null , new HiveReaderSetting( null , null , false , false , false ) );
    assertEquals( setting.getNeedColumnIds() , null );
  }

  @Test
  public void T_getColumnNames_1(){
    HiveVectorizedReaderSetting setting = new HiveVectorizedReaderSetting( null , null , null , null , null , null , new HiveReaderSetting( null , null , false , false , false ) );
    assertEquals( setting.getColumnNames() , null );
  }

}
