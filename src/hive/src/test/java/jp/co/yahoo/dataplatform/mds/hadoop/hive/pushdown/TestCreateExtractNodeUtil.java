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
package jp.co.yahoo.dataplatform.mds.hadoop.hive.pushdown;

import java.util.List;
import java.util.ArrayList;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import jp.co.yahoo.dataplatform.mds.spread.expression.IExtractNode;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.DataProvider;

import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.udf.generic.*;

public class TestCreateExtractNodeUtil{

  private ExprNodeDesc createExprNodeGenericFuncDesc(){
    GenericUDFIndex udfIndex = new GenericUDFIndex();
    List<ExprNodeDesc> childList = new ArrayList<ExprNodeDesc>();
    childList.add( new ExprNodeColumnDesc( TypeInfoFactory.booleanTypeInfo , "col1" , "col1" , false  ) );
    childList.add( new ExprNodeConstantDesc( TypeInfoFactory.stringTypeInfo , "child_name" ) );
    ExprNodeGenericFuncDesc funcDesc = new ExprNodeGenericFuncDesc( TypeInfoFactory.booleanTypeInfo , udfIndex , childList );
    return funcDesc;
  }

  @DataProvider(name = "T_getExtractNode_1")
  public Object[][] data1() {
    return new Object[][] {
      { new ExprNodeColumnDesc( TypeInfoFactory.booleanTypeInfo , "col1" , "col1" , false  ) },
      { new ExprNodeFieldDesc( TypeInfoFactory.booleanTypeInfo , new ExprNodeColumnDesc() , "col1" , false ) },
      { createExprNodeGenericFuncDesc() },
    };
  }
  

  @Test( dataProvider = "T_getExtractNode_1")
  public void T_getExtractNode_1( final ExprNodeDesc nodeDesc ){
    System.out.println( nodeDesc.toString() );
    assertTrue( CreateExtractNodeUtil.getExtractNode( nodeDesc ) instanceof IExtractNode);
  }

  @Test
  public void T_getExtractNode_2(){
    ExprNodeConstantDesc nodeDesc = new ExprNodeConstantDesc();
    Assert.assertEquals( CreateExtractNodeUtil.getExtractNode( nodeDesc ) , null );
  }

  @Test
  public void T_getExtractNodeFromGenericIndex_1(){
    GenericUDF udf = new GenericUDFTimestamp();
    List<ExprNodeDesc> childList = new ArrayList<ExprNodeDesc>();
    childList.add( new ExprNodeColumnDesc( TypeInfoFactory.booleanTypeInfo , "col1" , "col1" , false  ) );
    childList.add( new ExprNodeConstantDesc( TypeInfoFactory.stringTypeInfo , "child_name" ) );
    ExprNodeGenericFuncDesc funcDesc = new ExprNodeGenericFuncDesc( TypeInfoFactory.booleanTypeInfo , udf , childList );
    Assert.assertEquals( CreateExtractNodeUtil.getExtractNodeFromGenericIndex( funcDesc , udf ) , null );
  }

  @Test
  public void T_getExtractNodeFromGenericIndex_2(){
    GenericUDF udf = new GenericUDFTimestamp();
    List<ExprNodeDesc> childList = new ArrayList<ExprNodeDesc>();
    childList.add( new ExprNodeColumnDesc( TypeInfoFactory.booleanTypeInfo , "col1" , "col1" , false  ) );
    childList.add( new ExprNodeConstantDesc( TypeInfoFactory.stringTypeInfo , "child_name" ) );
    childList.add( new ExprNodeConstantDesc( TypeInfoFactory.stringTypeInfo , "child_name" ) );
    ExprNodeGenericFuncDesc funcDesc = new ExprNodeGenericFuncDesc( TypeInfoFactory.booleanTypeInfo , udf , childList );
    Assert.assertEquals( CreateExtractNodeUtil.getExtractNodeFromGenericIndex( funcDesc , udf ) , null );
  }

  @Test
  public void T_getExtractNodeFromGenericIndex_3(){
    GenericUDF udf = new GenericUDFTimestamp();
    List<ExprNodeDesc> childList = new ArrayList<ExprNodeDesc>();
    childList.add( new ExprNodeColumnDesc( TypeInfoFactory.booleanTypeInfo , "col1" , "col1" , false  ) );
    childList.add( new ExprNodeColumnDesc( TypeInfoFactory.booleanTypeInfo , "col1" , "col1" , false  ) );
    ExprNodeGenericFuncDesc funcDesc = new ExprNodeGenericFuncDesc( TypeInfoFactory.booleanTypeInfo , udf , childList );
    Assert.assertEquals( CreateExtractNodeUtil.getExtractNodeFromGenericIndex( funcDesc , udf ) , null );
  }

  @Test
  public void T_getExtractNodeFromGenericIndex_4(){
    MapTypeInfo info = new MapTypeInfo();
    info.setMapKeyTypeInfo( TypeInfoFactory.stringTypeInfo );
    info.setMapValueTypeInfo( TypeInfoFactory.stringTypeInfo );

    GenericUDF udf = new GenericUDFTimestamp();
    List<ExprNodeDesc> childList = new ArrayList<ExprNodeDesc>();
    childList.add( new ExprNodeColumnDesc( TypeInfoFactory.booleanTypeInfo , "col1" , "col1" , false  ) );
    childList.add( new ExprNodeConstantDesc( info , "child_name" ) );
    ExprNodeGenericFuncDesc funcDesc = new ExprNodeGenericFuncDesc( TypeInfoFactory.booleanTypeInfo , udf , childList );
    Assert.assertEquals( CreateExtractNodeUtil.getExtractNodeFromGenericIndex( funcDesc , udf ) , null );
  }

  @Test
  public void T_getExtractNodeFromGenericIndex_5(){
    GenericUDF udf = new GenericUDFTimestamp();
    List<ExprNodeDesc> childList = new ArrayList<ExprNodeDesc>();
    childList.add( new ExprNodeColumnDesc( TypeInfoFactory.booleanTypeInfo , "col1" , "col1" , false  ) );
    childList.add( new ExprNodeConstantDesc( TypeInfoFactory.booleanTypeInfo , "child_name" ) );
    ExprNodeGenericFuncDesc funcDesc = new ExprNodeGenericFuncDesc( TypeInfoFactory.booleanTypeInfo , udf , childList );
    Assert.assertEquals( CreateExtractNodeUtil.getExtractNodeFromGenericIndex( funcDesc , udf ) , null );
  }

}
