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
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.OrExpressionNode;
import org.testng.annotations.Test;

import org.apache.hadoop.fs.*;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.ql.plan.*;

import jp.co.yahoo.dataplatform.mds.*;

public class TestHiveReaderSetting{

  @Test
  public void T_createExpressionNode_1(){
    List<ExprNodeGenericFuncDesc> list = new ArrayList<ExprNodeGenericFuncDesc>();
    list.add( null );
    GenericUDF udf = new GenericUDFOPAnd();
    list.add( new ExprNodeGenericFuncDesc( PrimitiveObjectInspectorFactory.writableStringObjectInspector , udf , new ArrayList<ExprNodeDesc>() ) );

    HiveReaderSetting setting = new HiveReaderSetting( null , null , false );
    IExpressionNode node = setting.createExpressionNode( list );
    assertEquals( node.getClass().getName() , OrExpressionNode.class.getName() );
  }

  @Test
  public void T_createReadColumnNames_1(){
    HiveReaderSetting setting = new HiveReaderSetting( null , null , false );
    String readColumnJson = setting.createReadColumnNames( "a,b,,c" );
    assertEquals( readColumnJson , "[[\"a\"],[\"b\"],[\"c\"]]" );
  }

  @Test
  public void T_createReadColumnNames_2(){
    HiveReaderSetting setting = new HiveReaderSetting( null , null , false );
    String readColumnJson = setting.createReadColumnNames( "" );
    assertEquals( readColumnJson , null );
  }

  @Test
  public void T_createReadColumnNames_3(){
    HiveReaderSetting setting = new HiveReaderSetting( null , null , false );
    String readColumnJson = setting.createReadColumnNames( null );
    assertEquals( readColumnJson , null );
  }

  @Test
  public void T_createPathSet_1(){
    Path test = new Path( "file:///a/b/c" );
    HiveReaderSetting setting = new HiveReaderSetting( null , null , false );
    Set<String> set = setting.createPathSet( test );
    System.out.println( set.toString() );
    assertTrue( set.contains( "file:/a/b" ) );
    assertTrue( set.contains( "file:///a/b/c" ) );
    assertTrue( set.contains( "file:/a/b/c" ) );
  }

  @Test
  public void T_isVectorMode_1(){
    HiveReaderSetting setting = new HiveReaderSetting( null , null , true );
    assertTrue( setting.isVectorMode() );

    setting = new HiveReaderSetting( null , null , false );
    assertFalse( setting.isVectorMode() );
  }

  @Test
  public void T_getReaderConfig_1(){
    HiveReaderSetting setting = new HiveReaderSetting( null , null , true );
    assertEquals( null , setting.getReaderConfig() );
  }

  @Test
  public void T_getExpressionNode_1(){
    HiveReaderSetting setting = new HiveReaderSetting( null , null , true );
    assertEquals( null , setting.getExpressionNode() );
  }

}
