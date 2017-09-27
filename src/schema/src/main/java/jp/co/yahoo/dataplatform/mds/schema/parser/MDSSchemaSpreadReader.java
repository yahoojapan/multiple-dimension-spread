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
package jp.co.yahoo.dataplatform.mds.schema.parser;

import java.io.IOException;

import java.util.List;

import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.spread.column.SpreadColumn;
import jp.co.yahoo.dataplatform.mds.spread.expression.AndExpressionNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionIndex;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.IndexFactory;
import jp.co.yahoo.dataplatform.schema.parser.IParser;
import jp.co.yahoo.dataplatform.schema.parser.IStreamReader;

public class MDSSchemaSpreadReader implements IStreamReader {

  private final ISettableIndexParser currentParser;
  private final IExpressionIndex currentIndexList;
  private int currentIndex;

  public MDSSchemaSpreadReader( final Spread spread ) throws IOException{
    SpreadColumn spreadColumn = new SpreadColumn( "root" );
    spreadColumn.setSpread( spread );

    IExpressionNode node = new AndExpressionNode();
    currentIndexList = IndexFactory.toExpressionIndex( spread , node.exec( spread ) );
    currentIndex = 0;
    currentParser = MDSParserFactory.get( spreadColumn , currentIndexList.get( currentIndex ) );
  }

  public MDSSchemaSpreadReader( final Spread spread , final IExpressionNode filterNode ) throws IOException{
    IExpressionNode node = new AndExpressionNode();
    if( filterNode != null ){
      node = filterNode;
    }
    SpreadColumn spreadColumn = new SpreadColumn( "root" );
    spreadColumn.setSpread( spread );

    currentIndexList = IndexFactory.toExpressionIndex( spread , node.exec( spread ) );
    currentIndex = 0;
    currentParser = MDSParserFactory.get( spreadColumn , currentIndexList.get( currentIndex ) );
  }

  @Override
  public boolean hasNext() throws IOException{
    return currentIndex != currentIndexList.size();
  }

  @Override
  public IParser next() throws IOException{
    if( currentIndex == currentIndexList.size() ){
      return null;
    }
    currentParser.setIndex( currentIndexList.get( currentIndex ) );
    currentIndex++;
    return currentParser;
  }

  @Override
  public void close() throws IOException{
  }

}
