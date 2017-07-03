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
package jp.co.yahoo.dataplatform.mds.spread.column;

import java.util.Map;
import java.util.HashMap;

import jp.co.yahoo.dataplatform.mds.spread.Spread;

public class SpreadLink{

  private final Map<String,ICell> result;
  private final Spread spread;
  private final int index;

  public SpreadLink( final Spread spread , final int index ){
    this.spread = spread;
    this.index = index;
    result = new HashMap<String,ICell>();
  }

  public boolean containsColumn( final String columnName ){
    return spread.containsColumn( columnName );
  }

  public Map<String,ICell> getLine(){
    if( result.isEmpty() ){
      spread.getLine( result , index );
    }
    return result;
  }


}
