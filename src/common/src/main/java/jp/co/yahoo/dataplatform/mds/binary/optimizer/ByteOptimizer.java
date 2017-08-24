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
import jp.co.yahoo.dataplatform.mds.binary.maker.DumpByteColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.binary.maker.RangeDumpByteColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.binary.maker.UniqByteColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.binary.maker.RangeIndexByteColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.ByteColumnAnalizeResult;

public class ByteOptimizer implements IOptimizer{

  private final IColumnBinaryMaker dumpColumnBinaryMaker;
  private final IColumnBinaryMaker rangeDumpColumnBinaryMaker;
  private final IColumnBinaryMaker uniqColumnBinaryMaker;
  private final IColumnBinaryMaker rangeUniqColumnBinaryMaker;

  public ByteOptimizer( final Configuration config ) throws IOException{
    dumpColumnBinaryMaker = FindColumnBinaryMaker.get( DumpByteColumnBinaryMaker.class.getName() );
    rangeDumpColumnBinaryMaker = FindColumnBinaryMaker.get( RangeDumpByteColumnBinaryMaker.class.getName() );
    uniqColumnBinaryMaker = FindColumnBinaryMaker.get( UniqByteColumnBinaryMaker.class.getName() );
    rangeUniqColumnBinaryMaker = FindColumnBinaryMaker.get( RangeIndexByteColumnBinaryMaker.class.getName() );
  }

  @Override
  public ColumnBinaryMakerConfig getColumnBinaryMakerConfig( final ColumnBinaryMakerConfig commonConfig , final IColumnAnalizeResult analizeResult ){
    ColumnBinaryMakerConfig currentConfig = new ColumnBinaryMakerConfig( commonConfig );
    ByteColumnAnalizeResult castColumnAnalizeResult = (ByteColumnAnalizeResult)analizeResult;
    IColumnBinaryMaker makerClass;
    if( castColumnAnalizeResult.maybeSorted() ){
      int dump = rangeDumpColumnBinaryMaker.calcBinarySize( analizeResult );
      int uniq = rangeUniqColumnBinaryMaker.calcBinarySize( analizeResult );
      if( dump < uniq ){
        makerClass = rangeDumpColumnBinaryMaker;
      }
      else{
        makerClass = rangeUniqColumnBinaryMaker;
      }
    }
    else{
      int dump = dumpColumnBinaryMaker.calcBinarySize( analizeResult );
      int uniq = uniqColumnBinaryMaker.calcBinarySize( analizeResult );
      if( dump < uniq ){
        makerClass = dumpColumnBinaryMaker;
      }
      else{
        makerClass = uniqColumnBinaryMaker;
      }
    }
    if( currentConfig.byteMakerClass.getClass().getName().equals( makerClass.getClass().getName() ) ){
      return null;
    }
    currentConfig.byteMakerClass = makerClass;
    return currentConfig;
  }

}
