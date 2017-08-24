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
import java.io.UncheckedIOException;

import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;

public final class Analyzer{

  private Analyzer(){}

  public static List<IColumnAnalizer> getAnalizer( final Spread spread ) throws IOException{
    List<IColumnAnalizer> result = new ArrayList<IColumnAnalizer>();
    for( int i = 0 ; i < spread.getColumnSize() ; i++ ){
      IColumn column = spread.getColumn( i );
      result.add( ColumnAnalizerFactory.get( column ) );
    }
    return result;
  }

  public static List<IColumnAnalizeResult> analize( final Spread spread ) throws IOException{
    List<IColumnAnalizeResult> result = new ArrayList<IColumnAnalizeResult>();
    for( IColumnAnalizer analizer : getAnalizer( spread ) ){
      if( analizer != null ){
        result.add( analizer.analize() );
      }
    }
    return result;
  }

}
