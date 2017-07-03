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

import java.util.Map;
import java.util.HashMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.BackwardMatchStringFilter;
import jp.co.yahoo.dataplatform.mds.spread.expression.AndExpressionNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.ExecuterNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.StringExtractNode;
import org.testng.annotations.Test;

import jp.co.yahoo.dataplatform.schema.objects.*;
import jp.co.yahoo.dataplatform.schema.parser.IParser;
import jp.co.yahoo.dataplatform.mds.*;

public class TestMDSSchemaSpreadReader{

  @Test
  public void T_parser_1() throws IOException{
    Spread spread = new Spread();
    Map<String,Object> data = new HashMap<String,Object>();
    data.put( "col3" , new StringObj( "a" ) );
    spread.addRow( data );
    data.put( "col3" , new StringObj( "b" ) );
    spread.addRow( data );

    IExpressionNode node = new AndExpressionNode();
    node.addChildNode( new ExecuterNode( new StringExtractNode( "col3" ) , new BackwardMatchStringFilter( "a" ) ) );
    MDSSchemaSpreadReader reader = new MDSSchemaSpreadReader( spread , node );
    IParser parser = reader.next();
    PrimitiveObject a = parser.get( "col3" );
    assertEquals( "a" , a.getString() );
    parser = reader.next();
    a = parser.get( "col3" );
    assertEquals( "b" , a.getString() );
    reader.close();
  }

  @Test
  public void T_parser_2() throws IOException{
    Spread spread = new Spread();
    Map<String,Object> data = new HashMap<String,Object>();
    data.put( "col3" , new StringObj( "a" ) );
    spread.addRow( data );
    data.put( "col3" , new StringObj( "b" ) );
    spread.addRow( data );

    MDSSchemaSpreadReader reader = new MDSSchemaSpreadReader( spread , null );
    IParser parser = reader.next();
    PrimitiveObject a = parser.get( "col3" );
    assertEquals( "a" , a.getString() );
    parser = reader.next();
    a = parser.get( "col3" );
    assertEquals( "b" , a.getString() );
    reader.close();
  }

  @Test
  public void T_parser_3() throws IOException{
    Spread spread = new Spread();
    Map<String,Object> data = new HashMap<String,Object>();
    data.put( "col3" , new StringObj( "a" ) );
    spread.addRow( data );
    data.put( "col3" , new StringObj( "b" ) );
    spread.addRow( data );

    MDSSchemaSpreadReader reader = new MDSSchemaSpreadReader( spread );
    IParser parser = reader.next();
    PrimitiveObject a = parser.get( "col3" );
    assertEquals( "a" , a.getString() );
    parser = reader.next();
    a = parser.get( "col3" );
    assertEquals( "b" , a.getString() );
    reader.close();
  }

}
