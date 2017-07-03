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
import java.io.InputStream;

import java.util.List;

import jp.co.yahoo.dataplatform.mds.MDSReader;
import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.spread.column.SpreadColumn;
import jp.co.yahoo.dataplatform.mds.spread.expression.AndExpressionNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionIndex;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.IndexFactory;
import jp.co.yahoo.dataplatform.config.Configuration;
import jp.co.yahoo.dataplatform.schema.parser.IParser;
import jp.co.yahoo.dataplatform.schema.parser.IStreamReader;

public class MDSSchemaReader implements IStreamReader {

  private final MDSReader currentReader = new MDSReader();
  private final SpreadColumn spreadColumn = new SpreadColumn( "root" );

  private ISettableIndexParser currentParser;
  private IExpressionNode node = new AndExpressionNode();
  private IExpressionIndex currentIndexList;
  private Spread currentSpread;
  private int currentIndex;

  public void setNewStream( final InputStream in , final long dataSize, final Configuration config ) throws IOException{
    currentReader.setNewStream( in , dataSize , config );
    nextReader();
  }

  public void setNewStream( final InputStream in , final long dataSize, final Configuration config , final long start , final long length ) throws IOException{
    currentReader.setNewStream( in , dataSize , config , start , length );
    nextReader();
  }

  public void setExpressionNode( final IExpressionNode node ){
    if( node == null ){
      this.node = new AndExpressionNode();
    }
    else{
      this.node = node;
    }
  }

  private boolean nextReader() throws IOException{
    if( ! currentReader.hasNext() ){
      currentSpread = null;
      currentIndex = 0;
      return false;
    }
    currentSpread = currentReader.next();
    if( currentSpread.size() == 0 ){
      return nextReader();
    }
    List<Integer> indexList = node.exec( currentSpread );
    if( indexList != null && indexList.isEmpty() ){
      return nextReader();
    }
    currentIndexList = IndexFactory.toExpressionIndex( currentSpread , indexList );
    currentIndex = 0;

    spreadColumn.setSpread( currentSpread );
    currentParser = MDSParserFactory.get( spreadColumn , currentIndexList.get( currentIndex ) );
    return true;
  }


  @Override
  public boolean hasNext() throws IOException{
    if( currentSpread == null || currentIndex == currentIndexList.size() ){
      if( ! nextReader() ){
        return false;
      }
    }
    return true;
  }

  @Override
  public IParser next() throws IOException{
    if( currentSpread == null || currentIndex == currentIndexList.size() ){
      if( ! nextReader() ){
        return null;
      }
    }
    currentParser.setIndex( currentIndexList.get( currentIndex ) );
    currentIndex++;
    return currentParser;
  }

  @Override
  public void close() throws IOException{
    currentReader.close();
  }

}
