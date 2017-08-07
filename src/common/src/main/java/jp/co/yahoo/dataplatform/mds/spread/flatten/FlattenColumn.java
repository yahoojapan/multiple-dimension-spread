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
package jp.co.yahoo.dataplatform.mds.spread.flatten;

import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;

public class FlattenColumn{

  private final String linkName;
  private final String[] targetColumnNameArray;

  public FlattenColumn( final String linkName , final String[] targetColumnNameArray ){
    this.linkName = linkName;
    this.targetColumnNameArray = targetColumnNameArray;
  }

  public String getLinkName(){
    return linkName;
  }

  public String[] getFilterColumnNameArray(){
    return targetColumnNameArray;
  }

  public IColumn getColumn( final Spread spread ){
    IColumn currentColumn = null;
    for( String nodeName : targetColumnNameArray ){
      if( currentColumn == null ){
        currentColumn = spread.getColumn( nodeName );
      }
      else{
        if( currentColumn.getColumnType() == ColumnType.UNION ){
          currentColumn = currentColumn.getColumn( ColumnType.SPREAD );
        }
        currentColumn = currentColumn.getColumn( nodeName );
      }
    }
    currentColumn.setColumnName( linkName );
    return currentColumn;
  }

  public void flattenIndexNode( final BlockIndexNode rootNode ){
    BlockIndexNode currentNode = rootNode;
    for( String nodeName : targetColumnNameArray ){
      currentNode = currentNode.getChildNode( nodeName );
    }
    rootNode.putChildNode( linkName , currentNode );
  }

}
