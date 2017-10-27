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
package jp.co.yahoo.dataplatform.mds.blackbox;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import jp.co.yahoo.dataplatform.mds.spread.column.filter.PerfectMatchStringFilter;
import jp.co.yahoo.dataplatform.mds.spread.expression.*;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;
import jp.co.yahoo.dataplatform.schema.parser.IParser;
import jp.co.yahoo.dataplatform.schema.parser.JacksonMessageReader;
import jp.co.yahoo.dataplatform.config.Configuration;
import jp.co.yahoo.dataplatform.mds.MDSRecordWriter;
import jp.co.yahoo.dataplatform.mds.MDSReader;
import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;

public class TestGetPrimitiveObjectArray{
  private ByteArrayOutputStream out;

  @BeforeClass
  public void setup() throws IOException{
    out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    config.set( "spread.column.maker.setting" , "{ \"column_name\" : \"root\" , \"string_maker_class\" : \"jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeDumpStringColumnBinaryMaker\" }" );
    try( MDSRecordWriter writer = new MDSRecordWriter( out , config ) ){

      JacksonMessageReader messageReader = new JacksonMessageReader();
      BufferedReader in = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResource("blackbox/TestGetPrimitiveObjectArray.json").openStream()));
      String line = in.readLine();
      while (line != null) {
        IParser parser = messageReader.create(line);
        writer.addParserRow(parser);
        line = in.readLine();
      }
    }
  }

  @Test
  public void T_1() throws IOException{
    try(MDSReader reader = new MDSReader()) {
      Configuration readerConfig = new Configuration();
      byte[] data = out.toByteArray();
      InputStream fileIn = new ByteArrayInputStream(data);
      reader.setNewStream(fileIn, data.length, readerConfig);
      while (reader.hasNext()) {
        //IExpressionNode node = new AndExpressionNode();
        //node.addChildNode( new ExecuterNode( new StringExtractNode( "key1" ) , new PerfectMatchStringFilter( "a" ) ) );
        Spread spread = reader.next();
        IColumn key1Column = spread.getColumn("key1");
        IExpressionIndex indexList = new AllExpressionIndex(spread.size());
        PrimitiveObject[] primitiveArray = key1Column.getPrimitiveObjectArray(indexList, 0, spread.size());
        assertEquals(7, primitiveArray.length);
        assertEquals("a", primitiveArray[0].getString());
        assertEquals("b", primitiveArray[1].getString());
        assertEquals("a", primitiveArray[2].getString());
        assertEquals("b", primitiveArray[3].getString());
        assertEquals("a", primitiveArray[4].getString());
        assertEquals("b", primitiveArray[5].getString());
        assertEquals("a", primitiveArray[6].getString());
      }
    }
  }

  @Test
  public void T_2() throws IOException{
    try(MDSReader reader = new MDSReader()){
    Configuration readerConfig = new Configuration();
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    while( reader.hasNext() ) {
      IExpressionNode node = new AndExpressionNode();
      node.addChildNode(new ExecuterNode(new StringExtractNode("key1"), new PerfectMatchStringFilter("a")));
      Spread spread = reader.next();
      IExpressionIndex indexList = IndexFactory.toExpressionIndex(spread, node.exec(spread));
      IColumn key1Column = spread.getColumn("key1");
      PrimitiveObject[] primitiveArray = key1Column.getPrimitiveObjectArray(indexList, 0, indexList.size());
      assertEquals(4, primitiveArray.length);
      assertEquals("a", primitiveArray[0].getString());
      assertEquals("a", primitiveArray[1].getString());
      assertEquals("a", primitiveArray[2].getString());
      assertEquals("a", primitiveArray[3].getString());

    }
    }
  }

  @Test
  public void T_3() throws IOException{
    try(MDSReader reader = new MDSReader()) {
      Configuration readerConfig = new Configuration();
      readerConfig.set("spread.reader.expand.column", "{ \"base\" : { \"node\" : \"key2\" , \"link_name\" : \"expand_key2\" } }");
      byte[] data = out.toByteArray();
      InputStream fileIn = new ByteArrayInputStream(data);
      reader.setNewStream(fileIn, data.length, readerConfig);
      while (reader.hasNext()) {
        IExpressionNode node = new AndExpressionNode();
        node.addChildNode(new ExecuterNode(new StringExtractNode("key1"), new PerfectMatchStringFilter("a")));
        Spread spread = reader.next();
        IExpressionIndex indexList = IndexFactory.toExpressionIndex(spread, node.exec(spread));
        IColumn key1Column = spread.getColumn("expand_key2");
        PrimitiveObject[] primitiveArray = key1Column.getPrimitiveObjectArray(indexList, 0, indexList.size());
        assertEquals(9, primitiveArray.length);
        assertEquals(1, primitiveArray[0].getInt());
        assertEquals(2, primitiveArray[1].getInt());
        assertEquals(3, primitiveArray[2].getInt());
        assertEquals(6, primitiveArray[3].getInt());
        assertEquals(7, primitiveArray[4].getInt());
        assertEquals(8, primitiveArray[5].getInt());
        assertEquals(13, primitiveArray[6].getInt());
        assertEquals(14, primitiveArray[7].getInt());
        assertEquals(15, primitiveArray[8].getInt());
      }
    }
  }

  @Test
  public void T_4() throws IOException{
    try(MDSReader reader = new MDSReader()) {
      Configuration readerConfig = new Configuration();
      readerConfig.set("spread.reader.expand.column", "{ \"base\" : { \"node\" : \"key2\" , \"link_name\" : \"expand_key2\" } }");
      byte[] data = out.toByteArray();
      InputStream fileIn = new ByteArrayInputStream(data);
      reader.setNewStream(fileIn, data.length, readerConfig);
      while (reader.hasNext()) {
        IExpressionNode node = new AndExpressionNode();
        //node.addChildNode( new ExecuterNode( new StringExtractNode( "key1" ) , new PerfectMatchStringFilter( "a" ) ) );
        Spread spread = reader.next();
        IExpressionIndex indexList = IndexFactory.toExpressionIndex(spread, node.exec(spread));
        IColumn key1Column = spread.getColumn("key1");
        PrimitiveObject[] primitiveArray = key1Column.getPrimitiveObjectArray(indexList, 0, indexList.size());
        assertEquals(15, primitiveArray.length);
        assertEquals("a", primitiveArray[0].getString());
        assertEquals("a", primitiveArray[1].getString());
        assertEquals("a", primitiveArray[2].getString());
        assertEquals("b", primitiveArray[3].getString());
        assertEquals("b", primitiveArray[4].getString());
        assertEquals("a", primitiveArray[5].getString());
        assertEquals("a", primitiveArray[6].getString());
        assertEquals("a", primitiveArray[7].getString());
        assertEquals("b", primitiveArray[8].getString());
        assertEquals("b", primitiveArray[9].getString());
        assertEquals("b", primitiveArray[10].getString());
        assertEquals("b", primitiveArray[11].getString());
        assertEquals("a", primitiveArray[12].getString());
        assertEquals("a", primitiveArray[13].getString());
        assertEquals("a", primitiveArray[14].getString());
      }
    }
  }

  @Test
  public void T_5() throws IOException {
    try (MDSReader reader = new MDSReader()) {
      Configuration readerConfig = new Configuration();
      readerConfig.set("spread.reader.expand.column", "{ \"base\" : { \"node\" : \"key2\" , \"link_name\" : \"expand_key2\" } }");
      byte[] data = out.toByteArray();
      InputStream fileIn = new ByteArrayInputStream(data);
      reader.setNewStream(fileIn, data.length, readerConfig);
      while (reader.hasNext()) {
        IExpressionNode node = new AndExpressionNode();
        node.addChildNode(new ExecuterNode(new StringExtractNode("key1"), new PerfectMatchStringFilter("a")));
        Spread spread = reader.next();
        IExpressionIndex indexList = IndexFactory.toExpressionIndex(spread, node.exec(spread));
        IColumn key1Column = spread.getColumn("key1");
        PrimitiveObject[] primitiveArray = key1Column.getPrimitiveObjectArray(indexList, 0, indexList.size());
        assertEquals(9, primitiveArray.length);
        assertEquals("a", primitiveArray[0].getString());
        assertEquals("a", primitiveArray[1].getString());
        assertEquals("a", primitiveArray[2].getString());
        assertEquals("a", primitiveArray[3].getString());
        assertEquals("a", primitiveArray[4].getString());
        assertEquals("a", primitiveArray[5].getString());
        assertEquals("a", primitiveArray[6].getString());
        assertEquals("a", primitiveArray[7].getString());
        assertEquals("a", primitiveArray[8].getString());
      }
    }
  }
}
