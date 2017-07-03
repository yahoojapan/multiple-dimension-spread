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

import jp.co.yahoo.dataplatform.mds.spread.expression.ExecuterNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExtractNode;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIndex;

import jp.co.yahoo.dataplatform.mds.spread.column.filter.BooleanFilter;

public class BooleanHiveExpr implements IHiveExprNode{

  private final IExtractNode extractNode;

  public BooleanHiveExpr( final ExprNodeDesc nodeDesc ){
    extractNode = CreateExtractNodeUtil.getExtractNode( nodeDesc );
  }

  public BooleanHiveExpr( final ExprNodeGenericFuncDesc exprNodeDesc , final GenericUDFIndex udf ){
    extractNode = CreateExtractNodeUtil.getExtractNodeFromGenericIndex( exprNodeDesc , udf );
  }

  @Override
  public void addChildNode( final ExprNodeGenericFuncDesc exprNodeDesc ){
    throw new UnsupportedOperationException( "IHiveExprNode node can not have child node." );
  }

  @Override
  public IExpressionNode getPushDownFilterNode(){
    if( extractNode == null ){
      return null;
    }
    return new ExecuterNode( extractNode , new BooleanFilter( true ) );
  }

}
