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

import java.io.Serializable;

import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Properties;

import jp.co.yahoo.dataplatform.mds.hadoop.hive.pushdown.HiveExprOrNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionNode;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileSplit;

import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;

import jp.co.yahoo.dataplatform.config.Configuration;

public class HiveReaderSetting implements IReaderSetting{

  private final Configuration config;
  private final IExpressionNode node;
  private final boolean isVectorModeFlag;

  public HiveReaderSetting( final Configuration config , final IExpressionNode node , final boolean isVectorModeFlag ){
    this.config = config;
    this.node = node;
    this.isVectorModeFlag = isVectorModeFlag;
  }

  public HiveReaderSetting( final FileSplit split, final JobConf job ){
    config = new Configuration();

    Set<String> pathNameSet= createPathSet( split.getPath() );
    List<ExprNodeGenericFuncDesc> filterExprs = new ArrayList<ExprNodeGenericFuncDesc>();
    String filterExprSerialized = job.get( TableScanDesc.FILTER_EXPR_CONF_STR );
    if( filterExprSerialized != null ){
      filterExprs.add( SerializationUtilities.deserializeExpression(filterExprSerialized) );
    }

    MapWork mapWork;
    try{
      mapWork = Utilities.getMapWork(job);
    }catch( Exception e ){
      mapWork = null;
    }

    if( mapWork == null ){
      node = createExpressionNode( filterExprs );
      isVectorModeFlag = false;
      return;
    }

    node = createExpressionNode( filterExprs );

    for( Map.Entry<String,PartitionDesc> pathsAndParts: mapWork.getPathToPartitionInfo().entrySet() ){
      if( ! pathNameSet.contains( pathsAndParts.getKey() ) ){
        continue;
      }
      Properties props = pathsAndParts.getValue().getTableDesc().getProperties();
      if( props.containsKey( "mds.expand" ) ){
        config.set( "spread.reader.expand.column" , props.getProperty( "mds.expand" ) );
      }
      if( props.containsKey( "mds.flatten" ) ){
        config.set( "spread.reader.flatten.column" , props.getProperty( "mds.flatten" ) );
      }
    }

    config.set( "spread.reader.read.column.names" , createReadColumnNames( job.get( ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR , null ) ) );

    // Next Hive vesion;
    // Utilities.getUseVectorizedInputFileFormat(job)
    isVectorModeFlag = Utilities.isVectorMode( job );
  }

  public String createReadColumnNames( final String readColumnNames ){
    if( readColumnNames == null || readColumnNames.isEmpty() ){
      return null;
    }
    StringBuilder jsonStringBuilder = new StringBuilder();
    jsonStringBuilder.append( "[" );
    int addCount = 0;
    for( String readColumnName : readColumnNames.split( "," ) ){
      if( readColumnName.isEmpty() ){
        continue;
      }
      if( addCount != 0 ){
        jsonStringBuilder.append( "," );
      }
      jsonStringBuilder.append( "[\"" );
      jsonStringBuilder.append( readColumnName );
      jsonStringBuilder.append( "\"]" );
      addCount++;
    }
    jsonStringBuilder.append( "]" );
    return jsonStringBuilder.toString();
  }

  public IExpressionNode createExpressionNode( final List<ExprNodeGenericFuncDesc> filterExprs ){
    HiveExprOrNode hiveOrNode = new HiveExprOrNode();
    for( ExprNodeGenericFuncDesc filterExpr : filterExprs ){
      if( filterExpr != null ){
        hiveOrNode.addChildNode( filterExpr );
      }
    }

    return hiveOrNode.getPushDownFilterNode();
  }

  public Set<String> createPathSet( final Path target ){
    Set<String> result = new HashSet<String>();
    result.add( target.toString() );
    result.add( target.toUri().toString() );
    result.add( target.getParent().toUri().toString() );

    return result;
  }

  @Override
  public boolean isVectorMode(){
    return isVectorModeFlag;
  }

  @Override
  public Configuration getReaderConfig(){
    return config;
  }

  @Override
  public IExpressionNode getExpressionNode(){
    return node;
  }

}
