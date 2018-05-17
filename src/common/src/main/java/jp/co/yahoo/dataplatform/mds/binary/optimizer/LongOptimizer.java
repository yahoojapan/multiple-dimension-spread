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

import jp.co.yahoo.dataplatform.config.Configuration;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.FindColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.IColumnAnalizeResult;

public class LongOptimizer implements IOptimizer{

  private final IColumnBinaryMaker uniqMaker;
  private final IColumnBinaryMaker[] makerArray;

  public LongOptimizer( final Configuration config ) throws IOException{
    uniqMaker = FindColumnBinaryMaker.get( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeLongColumnBinaryMaker" );
    makerArray = new IColumnBinaryMaker[]{
      FindColumnBinaryMaker.get( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeDumpLongColumnBinaryMaker" ),
    };
  }

  @Override
  public ColumnBinaryMakerConfig getColumnBinaryMakerConfig( final ColumnBinaryMakerConfig commonConfig , final IColumnAnalizeResult analizeResult ){
    IColumnBinaryMaker maker = null;
    if( ( (double)analizeResult.getUniqCount() / (double)analizeResult.getRowCount() ) < 0.7d ){
      maker = uniqMaker;
    }
    else{
      int minSize = Integer.MAX_VALUE;
      for( IColumnBinaryMaker currentMaker : makerArray ){
        int currentSize = currentMaker.calcBinarySize( analizeResult );
        if( currentSize <= minSize ){
          maker = currentMaker;
          minSize = currentSize;
        }
      }
    }
    ColumnBinaryMakerConfig currentConfig = new ColumnBinaryMakerConfig( commonConfig );
    if( maker != null ){
      currentConfig.longMakerClass = maker;
    }
    return currentConfig;
  }

}
