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

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIndex;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.UDFLike;

import jp.co.yahoo.dataplatform.mds.spread.column.filter.NumberFilterType;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.StringCompareFilterType;

public final class HiveExprFactory {

  private HiveExprFactory(){}

  public static IHiveExprNode get( final ExprNodeGenericFuncDesc exprNodeDesc , final GenericUDF udf , final List<ExprNodeDesc> childNodeDesc ){
    if( udf instanceof GenericUDFBridge ){
      return getFromUdfClassName( ( (GenericUDFBridge)udf ).getUdfClass() , childNodeDesc );
    }

    if( udf instanceof GenericUDFOPEqual ){
      return new EqualsHiveExpr( childNodeDesc );
    }
    else if( udf instanceof GenericUDFOPNotEqual ){
      return new NotEqualsHiveExpr( childNodeDesc );
    }
    else if( udf instanceof GenericUDFOPNotNull ){
      return new NotNullHiveExpr( childNodeDesc );
    }
    else if( udf instanceof GenericUDFIn ){
      return new InHiveExpr( childNodeDesc );
    }
    else if( udf instanceof GenericUDFBetween ){
      return new BetweenHiveExpr( childNodeDesc );
    }
    else if( udf instanceof GenericUDFOPNull ){
      return new NullHiveExpr( childNodeDesc );
    }
    else if( udf instanceof GenericUDFIndex ){
      return new BooleanHiveExpr( exprNodeDesc , (GenericUDFIndex)udf );
    }
    else if( udf instanceof GenericUDFOPEqualOrLessThan ){
      return new CompareHiveExpr( childNodeDesc , StringCompareFilterType.LE , NumberFilterType.LE );
    }
    else if( udf instanceof GenericUDFOPLessThan ){
      return new CompareHiveExpr( childNodeDesc , StringCompareFilterType.LT , NumberFilterType.LT );
    }
    else if( udf instanceof GenericUDFOPEqualOrGreaterThan ){
      return new CompareHiveExpr( childNodeDesc , StringCompareFilterType.GE , NumberFilterType.GE );
    }
    else if( udf instanceof GenericUDFOPGreaterThan ){
      return new CompareHiveExpr( childNodeDesc , StringCompareFilterType.GT , NumberFilterType.GT );
    }

    return new UnsupportHiveExpr();
  }

  public static IHiveExprNode getFromUdfClassName( final Class<? extends UDF> udf , final List<ExprNodeDesc> childNodeDesc ){
    if( UDFLike.class.getName() == udf.getName() ){
      return new RegexpHiveExpr( childNodeDesc );
    }
    return new UnsupportHiveExpr();  
  }

}
