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
package jp.co.yahoo.dataplatform.mds.spread.analyzer;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;

import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;

public class SpreadColumnAnalizer implements IColumnAnalizer{

  private final IColumn column;

  public SpreadColumnAnalizer( final IColumn column ){
    this.column = column;
  }

  public IColumnAnalizeResult analize() throws IOException{
    List<IColumnAnalizeResult> resultList = new ArrayList<IColumnAnalizeResult>(); 
    for( IColumn childColumn : column.getListColumn() ){
      IColumnAnalizer analizer = ColumnAnalizerFactory.get( childColumn );
      resultList.add( analizer.analize() );
    }
    
    return new SpreadColumnAnalizeResult( column.getColumnName() , column.getColumnSize() , 0 , resultList );
  }

}
