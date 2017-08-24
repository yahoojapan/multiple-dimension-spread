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
package jp.co.yahoo.dataplatform.mds.binary.optimizer;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;

import jp.co.yahoo.dataplatform.mds.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;

public class BinaryMakerOptimizerNode{

  private final IColumnAnalizeResult analizeResult;
  private final Map<String,BinaryMakerOptimizerNode> childNodeMap;

  public BinaryMakerOptimizerNode( final IColumnAnalizeResult analizeResult ){
    this.analizeResult = analizeResult;
    childNodeMap = new HashMap<String,BinaryMakerOptimizerNode>();
    // TODO: getChild() 
  }

  public ColumnBinaryMakerCustomConfigNode createConfigNode( final ColumnBinaryMakerConfig commonConfig , final IOptimizerFactory factory ) throws IOException{
    IOptimizer optimizer = factory.get( analizeResult.getColumnType() );

    ColumnBinaryMakerConfig currentNodeConfig = optimizer.getColumnBinaryMakerConfig( commonConfig , analizeResult );
    if( currentNodeConfig == null ){
      return null;
    }
    // TODO: getChild
    return new ColumnBinaryMakerCustomConfigNode( analizeResult.getColumnName() , currentNodeConfig );
  }

}
