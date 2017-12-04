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

import java.io.IOException;

import java.util.List;

import jp.co.yahoo.dataplatform.mds.hadoop.hive.io.vector.ColumnVectorAssignorFactory;
import jp.co.yahoo.dataplatform.mds.hadoop.hive.io.vector.IColumnVectorAssignor;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionNode;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;

import jp.co.yahoo.dataplatform.config.Configuration;

public class HiveVectorizedReaderSetting implements IVectorizedReaderSetting{

  private final boolean[] projectionColumn;
  private final Object[] partitionValues;
  private final HiveReaderSetting hiveReaderConfig;
  private final VectorizedRowBatchCtx rbCtx;

  private final IColumnVectorAssignor[] assignors;
  private final int[] needColumnIds;
  private final String[] columnNames;

  public HiveVectorizedReaderSetting( final boolean[] projectionColumn , final Object[] partitionValues , final VectorizedRowBatchCtx rbCtx , final IColumnVectorAssignor[] assignors , final int[] needColumnIds , final String[] columnNames , final HiveReaderSetting hiveReaderConfig ){
    this.projectionColumn = projectionColumn;
    this.partitionValues = partitionValues;
    this.rbCtx = rbCtx;
    this.hiveReaderConfig = hiveReaderConfig;
    this.assignors = assignors;
    this.needColumnIds = needColumnIds;
    this.columnNames = columnNames;
  }

  public HiveVectorizedReaderSetting( final FileSplit split , final JobConf job , final HiveReaderSetting hiveReaderConfig ) throws IOException{
    this.hiveReaderConfig = hiveReaderConfig;

    rbCtx = Utilities.getVectorizedRowBatchCtx( job );
    partitionValues = new Object[rbCtx.getPartitionColumnCount()];
    if( 0 < partitionValues.length ){
      rbCtx.getPartitionValues( rbCtx, job, split, partitionValues );
    }

    TypeInfo[] typeInfos = rbCtx.getRowColumnTypeInfos();
    columnNames = rbCtx.getRowColumnNames();
    needColumnIds = createNeedColumnId( ColumnProjectionUtils.getReadColumnIDs( job ) );

    projectionColumn = new boolean[columnNames.length];
    assignors = new IColumnVectorAssignor[columnNames.length];
    for( int id : needColumnIds ){
      projectionColumn[id] = true;
      assignors[id] = ColumnVectorAssignorFactory.create( typeInfos[id] );
    }
  }

  public int[] createNeedColumnId( final List<Integer> needColumnIDList ){
    int[] result = new int[needColumnIDList.size()];
    for( int i = 0 ; i < result.length ; i++ ){
      result[i] = needColumnIDList.get(i).intValue();
    }
    return result;
  }

  @Override
  public boolean isVectorMode(){
    return hiveReaderConfig.isVectorMode();
  }

  @Override
  public boolean isDisableSkipBlock(){
    return hiveReaderConfig.isDisableSkipBlock();
  }

  @Override
  public boolean isDisableFilterPushdown(){
    return hiveReaderConfig.isDisableFilterPushdown();
  }

  @Override
  public Configuration getReaderConfig(){
    return hiveReaderConfig.getReaderConfig();
  }

  @Override
  public IExpressionNode getExpressionNode(){
    return hiveReaderConfig.getExpressionNode();
  }

  @Override
  public VectorizedRowBatch createVectorizedRowBatch(){
    return rbCtx.createVectorizedRowBatch( projectionColumn );
  }

  @Override
  public void setPartitionValues( final VectorizedRowBatch outputBatch ){
    if( 0 < partitionValues.length ){
      rbCtx.addPartitionColsToBatch( outputBatch , partitionValues );
    }
  }

  @Override
  public IColumnVectorAssignor[] getAssignors(){
    return assignors;
  }

  @Override
  public int[] getNeedColumnIds(){
    return needColumnIds;
  }

  @Override
  public String[] getColumnNames(){
    return columnNames;
  }

}
