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
import java.util.List;

import jp.co.yahoo.dataplatform.mds.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;

public class BinaryMakerOptimizer{

  private final Map<String,BinaryMakerOptimizerNode> childNodeMap;

  public BinaryMakerOptimizer( final List<IColumnAnalizeResult> analizeResultList ){
    childNodeMap = new HashMap<String,BinaryMakerOptimizerNode>( analizeResultList.size() );
    for( IColumnAnalizeResult analizeResult : analizeResultList ){
      childNodeMap.put( analizeResult.getColumnName() , new BinaryMakerOptimizerNode( analizeResult ) );
    }
  }

  public ColumnBinaryMakerCustomConfigNode createConfigNode( final ColumnBinaryMakerConfig commonConfig , final IOptimizerFactory factory ) throws IOException{
    ColumnBinaryMakerCustomConfigNode rootNode = new ColumnBinaryMakerCustomConfigNode( "root" , commonConfig );
    for( Map.Entry<String,BinaryMakerOptimizerNode> entry : childNodeMap.entrySet() ){
      ColumnBinaryMakerCustomConfigNode childNode = entry.getValue().createConfigNode( commonConfig , factory );
      if( childNode != null ){
        rootNode.addChildConfigNode( entry.getKey() , childNode );
      }
    }
    return rootNode;
  }

}
