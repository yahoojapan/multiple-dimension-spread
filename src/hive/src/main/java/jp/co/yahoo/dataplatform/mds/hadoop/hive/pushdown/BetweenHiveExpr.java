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

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableVoidObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantDoubleObjectInspector;

import jp.co.yahoo.dataplatform.schema.objects.ByteObj;
import jp.co.yahoo.dataplatform.schema.objects.ShortObj;
import jp.co.yahoo.dataplatform.schema.objects.IntegerObj;
import jp.co.yahoo.dataplatform.schema.objects.LongObj;
import jp.co.yahoo.dataplatform.schema.objects.FloatObj;
import jp.co.yahoo.dataplatform.schema.objects.DoubleObj;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.spread.expression.ExecuterNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExtractNode;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.IFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.BooleanFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.NullFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.NumberFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.NumberRangeFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.NumberFilterType;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.PerfectMatchStringFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.RangeStringCompareFilter;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.AndExpressionNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.OrExpressionNode;

public class BetweenHiveExpr implements IHiveExprNode{

  private final List<ExprNodeDesc> nodeDescList;

  public BetweenHiveExpr( final List<ExprNodeDesc> nodeDescList ){
    this.nodeDescList = nodeDescList;
  }

  public static IExpressionNode getNumberRangeExecuter( final boolean invert , final PrimitiveObject minObj , final PrimitiveObject maxObj , final IExtractNode targetColumn ){
    return new ExecuterNode( targetColumn , new NumberRangeFilter( invert , minObj , true , maxObj , true ) );
  }

  public static IExpressionNode getRangeExecuter( boolean invert , final PrimitiveObjectInspector minPrimitiveObjectInspector , final PrimitiveObjectInspector maxPrimitiveObjectInspector , final IExtractNode targetColumn ){
    switch( minPrimitiveObjectInspector.getPrimitiveCategory() ){
      case STRING:
        String minStr =  ( (WritableConstantStringObjectInspector)minPrimitiveObjectInspector ).getWritableConstantValue().toString();
        String maxStr =  ( (WritableConstantStringObjectInspector)maxPrimitiveObjectInspector ).getWritableConstantValue().toString();
        IFilter filter = new RangeStringCompareFilter( maxStr , true , minStr , true , invert );
        return new ExecuterNode( targetColumn , filter );
      case BYTE:
        return getNumberRangeExecuter( invert , new ByteObj( ( (WritableConstantByteObjectInspector)minPrimitiveObjectInspector ).getWritableConstantValue().get() ) , new ByteObj( ( (WritableConstantByteObjectInspector)maxPrimitiveObjectInspector ).getWritableConstantValue().get() ) , targetColumn );
      case SHORT:
        return getNumberRangeExecuter( invert , new ShortObj( ( (WritableConstantShortObjectInspector)minPrimitiveObjectInspector ).getWritableConstantValue().get() ) , new ShortObj( ( (WritableConstantShortObjectInspector)maxPrimitiveObjectInspector ).getWritableConstantValue().get() ) , targetColumn );
      case INT:
        return getNumberRangeExecuter( invert , new IntegerObj( ( (WritableConstantIntObjectInspector)minPrimitiveObjectInspector ).getWritableConstantValue().get() ) , new IntegerObj( ( (WritableConstantIntObjectInspector)maxPrimitiveObjectInspector ).getWritableConstantValue().get() ) , targetColumn );
      case LONG:
        return getNumberRangeExecuter( invert , new LongObj( ( (WritableConstantLongObjectInspector)minPrimitiveObjectInspector ).getWritableConstantValue().get() ) , new LongObj( ( (WritableConstantLongObjectInspector)maxPrimitiveObjectInspector ).getWritableConstantValue().get() ) , targetColumn );
      case FLOAT:
        return getNumberRangeExecuter( invert , new FloatObj( ( (WritableConstantFloatObjectInspector)minPrimitiveObjectInspector ).getWritableConstantValue().get() ) , new FloatObj( ( (WritableConstantFloatObjectInspector)maxPrimitiveObjectInspector ).getWritableConstantValue().get() ) , targetColumn );
      case DOUBLE:
        return getNumberRangeExecuter( invert , new DoubleObj( ( (WritableConstantDoubleObjectInspector)minPrimitiveObjectInspector ).getWritableConstantValue().get() ) , new DoubleObj( ( (WritableConstantDoubleObjectInspector)maxPrimitiveObjectInspector ).getWritableConstantValue().get() ) , targetColumn );
      default:
        return null;
    }
  }

  @Override
  public void addChildNode( final ExprNodeGenericFuncDesc exprNodeDesc ){
    throw new UnsupportedOperationException( "IHiveExprNode node can not have child node." );
  }

  @Override
  public IExpressionNode getPushDownFilterNode(){
    if( nodeDescList.size() != 4 ){
      return null;
    }
    ExprNodeDesc constNode1 = nodeDescList.get( 0 );
    ExprNodeDesc columnNode = nodeDescList.get( 1 );
    ExprNodeDesc constNode2 = nodeDescList.get( 2 );
    ExprNodeDesc constNode3 = nodeDescList.get( 3 );

    if( ! ( constNode1 instanceof ExprNodeConstantDesc ) || ! ( constNode2 instanceof ExprNodeConstantDesc ) || ! ( constNode3 instanceof ExprNodeConstantDesc ) ){
      return null;
    } 
    ExprNodeConstantDesc booleanNode = (ExprNodeConstantDesc)constNode1;
    ObjectInspector booleanOjectInspector = booleanNode.getWritableObjectInspector();
    if( booleanOjectInspector.getCategory() != ObjectInspector.Category.PRIMITIVE ){
      return null;
    }
    PrimitiveObjectInspector booleanPrimitiveObjectInspector = (PrimitiveObjectInspector)booleanOjectInspector;
    if( booleanPrimitiveObjectInspector.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN ){
      return null;
    }
    boolean invert = ( (WritableConstantBooleanObjectInspector)booleanPrimitiveObjectInspector ).getWritableConstantValue().get();

    ExprNodeConstantDesc minNode = (ExprNodeConstantDesc)constNode2;
    ExprNodeConstantDesc maxNode = (ExprNodeConstantDesc)constNode3;

    ObjectInspector minOjectInspector = minNode.getWritableObjectInspector();
    ObjectInspector maxOjectInspector = maxNode.getWritableObjectInspector();
    if( minOjectInspector.getCategory() != ObjectInspector.Category.PRIMITIVE || maxOjectInspector.getCategory() != ObjectInspector.Category.PRIMITIVE ){
      return null;
    }
    PrimitiveObjectInspector minPrimitiveObjectInspector = (PrimitiveObjectInspector)minOjectInspector;
    PrimitiveObjectInspector maxPrimitiveObjectInspector = (PrimitiveObjectInspector)maxOjectInspector;
    if( minPrimitiveObjectInspector.getPrimitiveCategory() != maxPrimitiveObjectInspector.getPrimitiveCategory() ){
      return null;
    }

    IExtractNode extractNode = CreateExtractNodeUtil.getExtractNode( columnNode );
    if( extractNode == null ){
      return null;
    }

    return getRangeExecuter( invert , minPrimitiveObjectInspector , maxPrimitiveObjectInspector , extractNode );
  }

}
