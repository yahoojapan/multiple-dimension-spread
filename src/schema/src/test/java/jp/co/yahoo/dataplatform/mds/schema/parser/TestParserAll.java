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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import static org.testng.Assert.assertEquals;

import jp.co.yahoo.dataplatform.mds.MDSWriter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.*;
import jp.co.yahoo.dataplatform.mds.spread.expression.AndExpressionNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.ExecuterNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.StringExtractNode;
import org.testng.annotations.Test;

import jp.co.yahoo.dataplatform.schema.objects.*;
import jp.co.yahoo.dataplatform.schema.parser.IParser;
import jp.co.yahoo.dataplatform.schema.parser.JacksonMessageReader;
import jp.co.yahoo.dataplatform.config.Configuration;
import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.schema.formatter.MDSSchemaStreamWriter;

public class TestParserAll{

  private InputStream readFile() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    MDSSchemaStreamWriter writer = new MDSSchemaStreamWriter( out , new Configuration() );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "parser/TestParserAll.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      writer.write( parser );
      line = in.readLine();
    }
    writer.close();

    return new ByteArrayInputStream( out.toByteArray() );
  }

  private InputStream createFile() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    MDSWriter writer = new MDSWriter( out , new Configuration() );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "parser/TestParserAll.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      Spread spread = new Spread();
      spread.addParserRow( parser );
      writer.append( spread );
      line = in.readLine();
    }
    writer.close();

    return new ByteArrayInputStream( out.toByteArray() );
  }

  @Test
  public void T_parse_1() throws IOException{
    MDSSchemaReader reader = new MDSSchemaReader();
    reader.setNewStream( readFile() , 1024 * 1024 * 2 , new Configuration() );
    while( reader.hasNext() ){
      IParser parser = reader.next();
      PrimitiveObject col1 = parser.get( "col1" );
      assertEquals( "string" , col1.getString() );
    }
    reader.close();
  }

  @Test
  public void T_parser_2() throws IOException{
    MDSSchemaReader reader = new MDSSchemaReader();
    reader.setNewStream( readFile() , 1024 * 1024 * 2 , new Configuration() );
    int i = 0;
    while( reader.hasNext() ){
      IParser parser = reader.next();
      IParser col3 = parser.getParser( "col3" );
      PrimitiveObject a = col3.get( "a" );
      if( i == 0 ){
        assertEquals( a.getBoolean() , true );
      }
      else if( i == 3 ){
        assertEquals( a.getBoolean() , false );
      }
      else{
        assertEquals( a , null );
      }
      i++;
    }
    reader.close();
  }

  @Test
  public void T_parser_3() throws IOException{
    MDSSchemaReader reader = new MDSSchemaReader();
    reader.setNewStream( readFile() , 1024 * 1024 * 2 , new Configuration() );
    while( reader.hasNext() ){
      IParser parser = reader.next();
      IParser col2 = parser.getParser( "col2" );
      for( int i = 0 ; i < col2.size() ; i++ ){
        PrimitiveObject a = col2.get(i);
        assertEquals( a.getInt() , i );
      }
    }
    reader.close();
  }

  @Test
  public void T_parser_4() throws IOException{
    MDSSchemaReader reader = new MDSSchemaReader();
    reader.setNewStream( readFile() , 1024 * 1024 * 2 , new Configuration() );
    while( reader.hasNext() ){
      IParser parser = reader.next();
      PrimitiveObject a = parser.get( "col5" );
      assertEquals( a , null );
    }
    reader.close();
  }

  @Test
  public void T_parser_5() throws IOException{
    MDSSchemaReader reader = new MDSSchemaReader();
    reader.setNewStream( readFile() , 1024 * 1024 * 2 , new Configuration() );
    while( reader.hasNext() ){
      IParser parser = reader.next();
      IParser parser2 = parser.getParser( "col5" );
      PrimitiveObject a = parser2.get( "a" );
      assertEquals( a , null );
    }
    reader.close();
  }

  @Test
  public void T_parser_6() throws IOException{
    MDSSchemaReader reader = new MDSSchemaReader();
    reader.setNewStream( readFile() , 1024 * 1024 * 2 , new Configuration() );
    while( reader.hasNext() ){
      IParser parser = reader.next();
      PrimitiveObject a = parser.get( "col3" );
      assertEquals( a , null );
    }
    reader.close();
  }

  @Test
  public void T_parser_7() throws IOException{
    MDSSchemaReader reader = new MDSSchemaReader();
    reader.setNewStream( createFile() , 1024 * 1024 * 2 , new Configuration() );
    while( reader.hasNext() ){
      IParser parser = reader.next();
    }
    reader.close();
  }

  @Test
  public void T_parser_8() throws IOException{
    MDSSchemaReader reader = new MDSSchemaReader();
    IExpressionNode node = new AndExpressionNode();
    StringExtractNode dNode = new StringExtractNode( "z" );
    node.addChildNode( new ExecuterNode( new StringExtractNode( "col3" , dNode ) , new PerfectMatchStringFilter( "a" ) ) );
    reader.setExpressionNode( node );
    Configuration config = new Configuration();
    reader.setNewStream( createFile() , 1024 * 1024 * 2 , config );
    while( reader.hasNext() ){
      IParser parser = reader.next();
      IParser col3 = parser.getParser( "col3" );
      PrimitiveObject a = col3.get( "z" );
      assertEquals( "a" , a.getString() );
    }
    reader.close();
  }

}
