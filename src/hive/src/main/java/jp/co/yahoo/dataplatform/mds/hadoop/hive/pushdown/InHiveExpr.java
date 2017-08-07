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

import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionNode;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

import jp.co.yahoo.dataplatform.mds.spread.expression.IExtractNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.OrExpressionNode;

public class InHiveExpr implements IHiveExprNode{

  private final List<ExprNodeDesc> nodeDescList;

  public InHiveExpr( final List<ExprNodeDesc> nodeDescList ){
    this.nodeDescList = nodeDescList;
  }

  @Override
  public void addChildNode( final ExprNodeGenericFuncDesc exprNodeDesc ){
    throw new UnsupportedOperationException( "IHiveExprNode node can not have child node." );
  }

  @Override
  public IExpressionNode getPushDownFilterNode(){
    if( nodeDescList.size() < 2 ){
      return null;
    }
    ExprNodeDesc columnDesc = nodeDescList.get( 0 );
    IExtractNode extractNode = CreateExtractNodeUtil.getExtractNode( columnDesc );
    if( extractNode == null ){
      return null;
    }
    IExpressionNode result = new OrExpressionNode();
    for( int i = 1 ; i < nodeDescList.size() ; i++ ){
      ExprNodeDesc exprNode = nodeDescList.get( i );
      if( ! ( exprNode instanceof ExprNodeConstantDesc ) ){
        return null;
      }
      ExprNodeConstantDesc constantDesc = (ExprNodeConstantDesc)exprNode;
      IExpressionNode equalsNode = EqualsHiveExpr.getEqualsExecuter( constantDesc , extractNode );
      if( equalsNode == null ){
        return null;
      }
      result.addChildNode( equalsNode );
    }
    return result;
  }

}
