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

import jp.co.yahoo.dataplatform.mds.spread.expression.IExtractNode;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIndex;

import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;

import jp.co.yahoo.dataplatform.mds.spread.expression.StringExtractNode;

public final class CreateExtractNodeUtil{

  private CreateExtractNodeUtil(){}

  public static IExtractNode getExtractNode(final ExprNodeDesc target ){
    if( target instanceof ExprNodeGenericFuncDesc ){
      return getExtractNodeFromGenericFunc( (ExprNodeGenericFuncDesc)target );
    }
    else if( target instanceof ExprNodeFieldDesc ){
      return getExtractNodeFromField( (ExprNodeFieldDesc)target  );
    }
    else if( target instanceof ExprNodeColumnDesc ){
      if( ( (ExprNodeColumnDesc)target ).getIsPartitionColOrVirtualCol() ){
        return null;
      }
      return getExtractNodeFromColumn( (ExprNodeColumnDesc)target  );
    }
    return null;
  }

  public static IExtractNode getExtractNodeFromGenericFunc( final ExprNodeGenericFuncDesc target ){
    GenericUDF udf = target.getGenericUDF();
    if( ! ( udf instanceof GenericUDFIndex ) ){
      return null;
    }
    return getExtractNodeFromGenericIndex( target , (GenericUDFIndex)udf );
  }

  public static IExtractNode getExtractNodeFromGenericIndex( final ExprNodeGenericFuncDesc exprNodeDesc , final GenericUDF udf ){
    if( ! ( udf instanceof GenericUDFIndex ) ){
      return null;
    }
    List<ExprNodeDesc> nodeDescList = exprNodeDesc.getChildren();
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

    ObjectInspector objectInspector = constantDesc.getWritableObjectInspector();
    if( objectInspector.getCategory() != ObjectInspector.Category.PRIMITIVE ){
      return null;
    }

    PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector)objectInspector;
    if( primitiveObjectInspector.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING ){
      return null;
    }
    IExtractNode childExtraNode = new StringExtractNode( ( (WritableConstantStringObjectInspector)primitiveObjectInspector ).getWritableConstantValue().toString() );

    IExtractNode parentExtraNode = getExtractNode( columnDesc );

    parentExtraNode.pushChild( childExtraNode );

    return parentExtraNode;
  }

  public static IExtractNode getExtractNodeFromField( final ExprNodeFieldDesc target ){
    IExtractNode parentExtraNode = getExtractNode( target.getDesc() );
    IExtractNode childExtraNode = new StringExtractNode( target.getFieldName() );
    
    parentExtraNode.pushChild( childExtraNode );
    return parentExtraNode;
  }

  public static IExtractNode getExtractNodeFromColumn( final ExprNodeColumnDesc target ){
    return new StringExtractNode( target.getColumn() );
  }

}
