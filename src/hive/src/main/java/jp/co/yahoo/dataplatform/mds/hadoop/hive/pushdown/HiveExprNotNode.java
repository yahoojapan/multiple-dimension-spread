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

import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.NotExpressionNode;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;

public class HiveExprNotNode implements IHiveExprNode{

  private final List<IHiveExprNode> childNodeList = new ArrayList<IHiveExprNode>();

  public HiveExprNotNode(){}

  public HiveExprNotNode( final List<ExprNodeDesc> childExprNodeDesc ){
    for( ExprNodeDesc nodeChild : childExprNodeDesc  ){
      if( nodeChild instanceof ExprNodeGenericFuncDesc ){
        addChildNode( (ExprNodeGenericFuncDesc)nodeChild );
      }
      else if( ( nodeChild instanceof ExprNodeColumnDesc ) || ( nodeChild instanceof ExprNodeFieldDesc ) ){
        childNodeList.add( new BooleanHiveExpr( nodeChild ) );
      }
      else{
        childNodeList.add( new UnsupportHiveExpr() );
      }
    }
  }

  @Override
  public void addChildNode( final ExprNodeGenericFuncDesc exprNodeDesc ){
    GenericUDF udf = exprNodeDesc.getGenericUDF();
    if( udf instanceof GenericUDFOPAnd ){
      childNodeList.add( new HiveExprAndNode( exprNodeDesc.getChildren() ) );
    }
    else if( udf instanceof GenericUDFOPOr ){
      childNodeList.add( new HiveExprOrNode( exprNodeDesc.getChildren() ) );
    }
    else if( udf instanceof GenericUDFOPNot ){
      childNodeList.add( new HiveExprNotNode( exprNodeDesc.getChildren() ) );
    }
    else{
      childNodeList.add( HiveExprFactory.get( exprNodeDesc , udf , exprNodeDesc.getChildren() ) );
    }
  }

  @Override
  public IExpressionNode getPushDownFilterNode(){
    IExpressionNode result = new NotExpressionNode();
    for( IHiveExprNode childHiveExprNode : childNodeList ){
      IExpressionNode childNode = childHiveExprNode.getPushDownFilterNode();
      if( childNode != null ){
        result.addChildNode( childNode );
      }
    }
    return result;
  }

}
