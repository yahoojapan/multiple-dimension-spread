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
package jp.co.yahoo.dataplatform.mds.spread.expand;

import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

public class LinkColumn{

  private final String baseColumnName;
  private final String linkName;
  private final String[] nodeNameArray;

  public LinkColumn( final String baseColumnName , final String linkName , final String[] nodeNameArray ){
    this.baseColumnName = baseColumnName;
    this.linkName = linkName;
    this.nodeNameArray = nodeNameArray;
  }

  public String getLinkName(){
    return linkName;
  }

  public String[] getNeedColumnNameArray(){
    return nodeNameArray;
  }

  public void createLink( final ExpandSpread expandSpread ){
    IColumn linkTargetColumn = null;
    for( String nodeName : nodeNameArray ){
      if( linkTargetColumn == null ){
        linkTargetColumn = expandSpread.getColumn( nodeName );
      }
      else{
        if( linkTargetColumn.getColumnType() == ColumnType.UNION ){
          linkTargetColumn = linkTargetColumn.getColumn( ColumnType.SPREAD );
        }
        linkTargetColumn = linkTargetColumn.getColumn( nodeName );
      }
    }
    if( linkTargetColumn.getColumnType() != ColumnType.ARRAY ){
      return;
    }

    IColumn column = expandSpread.getColumn( baseColumnName );
    if( column instanceof ExpandColumn ){
      ExpandColumn expandColumn = (ExpandColumn)column;
      int[] indexArray = expandColumn.getColumnIndexArray();
      expandSpread.addExpandColumn( linkName , linkTargetColumn.getColumn(0) , indexArray );
    }
    else{
      expandSpread.addExpandLeafColumn( linkName , linkTargetColumn.getColumn(0) );
    }
  }

}
