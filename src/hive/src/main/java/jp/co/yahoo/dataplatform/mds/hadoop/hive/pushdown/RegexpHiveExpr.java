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

import jp.co.yahoo.dataplatform.mds.spread.column.filter.IFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.RegexpMatchStringFilter;
import jp.co.yahoo.dataplatform.mds.spread.expression.ExecuterNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExtractNode;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.UDFLike;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;

public class RegexpHiveExpr implements IHiveExprNode{

  private final List<ExprNodeDesc> nodeDescList;

  public RegexpHiveExpr( final List<ExprNodeDesc> nodeDescList ){
    this.nodeDescList = nodeDescList;
  }

  public static IExpressionNode getRegexpExecuter(final ExprNodeConstantDesc constDesc , final IExtractNode targetColumn ){
    ObjectInspector objectInspector = constDesc.getWritableObjectInspector();
    if( objectInspector.getCategory() != ObjectInspector.Category.PRIMITIVE ){
      return null;
    }
    PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector)objectInspector;
    IFilter filter = null;
    switch( primitiveObjectInspector.getPrimitiveCategory() ){
      case STRING:
        String regexp = UDFLike.likePatternToRegExp( ( (WritableConstantStringObjectInspector)primitiveObjectInspector ).getWritableConstantValue().toString() );
        filter = new RegexpMatchStringFilter( "^" + regexp + "$" );
        break;
      case BINARY:
      case BOOLEAN:
      case BYTE:
      case DOUBLE:
      case FLOAT:
      case INT:
      case LONG:
      case SHORT:
      case DATE:
      case DECIMAL:
      case TIMESTAMP:
      case VOID:
      default:
        filter = null;
      break;
    }
    if( filter == null ){
      return null;
    }
    return new ExecuterNode( targetColumn , filter );
  }

  @Override
  public void addChildNode( final ExprNodeGenericFuncDesc exprNodeDesc ){
    throw new UnsupportedOperationException( "IHiveExprNode node can not have child node." );
  }

  @Override
  public IExpressionNode getPushDownFilterNode(){
    if( nodeDescList.size() != 2 ){
      return null;
    }
    ExprNodeDesc exprNode1 = nodeDescList.get( 0 );
    ExprNodeDesc exprNode2 = nodeDescList.get( 1 );

    ExprNodeDesc columnDesc;
    ExprNodeConstantDesc constantDesc;

    if( exprNode1 instanceof ExprNodeConstantDesc ){
      columnDesc = exprNode2;
      constantDesc = (ExprNodeConstantDesc)exprNode1;
    } 
    else if( exprNode2 instanceof ExprNodeConstantDesc ){
      columnDesc = exprNode1;
      constantDesc = (ExprNodeConstantDesc)exprNode2;
    }
    else{
      return null;
    } 

    IExtractNode extractNode = CreateExtractNodeUtil.getExtractNode( columnDesc ); 
    if( extractNode == null ){
      return null;
    }

    return getRegexpExecuter( constantDesc , extractNode );
  }

}
