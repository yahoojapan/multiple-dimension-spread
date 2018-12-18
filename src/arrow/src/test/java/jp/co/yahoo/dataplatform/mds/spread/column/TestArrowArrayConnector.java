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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import java.util.List;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;

import jp.co.yahoo.dataplatform.config.Configuration;
import jp.co.yahoo.dataplatform.schema.objects.*;
import jp.co.yahoo.dataplatform.schema.parser.IParser;
import jp.co.yahoo.dataplatform.schema.parser.JacksonMessageReader;
import jp.co.yahoo.dataplatform.mds.MDSWriter;
import jp.co.yahoo.dataplatform.mds.MDSReader;
import jp.co.yahoo.dataplatform.mds.MDSRecordWriter;
import jp.co.yahoo.dataplatform.mds.MDSArrowReader;
import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.spread.ArrowSpreadUtil;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ArrayCell;

public class TestArrowArrayConnector{

  private byte[] createTestFile() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    MDSRecordWriter writer = new MDSRecordWriter( out , config );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "spread/column/array_test_data.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      writer.addParserRow( parser );
      line = in.readLine();
    }
    writer.close();
    return out.toByteArray();
  }

  @Test
  public void T_1() throws IOException{
    byte[] mdsFile = createTestFile();
    InputStream in = new ByteArrayInputStream( mdsFile );
    MDSReader reader = new MDSReader();
    Configuration config = new Configuration();
    reader.setNewStream( in , mdsFile.length , config );
    MDSArrowReader arrowReader = new MDSArrowReader( reader , config );

    File file = new File( "target/array_column_test.arrow" );
    if( file.exists() ){
      file.delete();
    }
    FileOutputStream out = new FileOutputStream( file );
    ArrowFileWriter writer = null;
    while( arrowReader.hasNext() ){
      ValueVector vector = arrowReader.next();
      if( writer == null ){
        VectorSchemaRoot schema = new VectorSchemaRoot( (FieldVector)vector );
        writer = new ArrowFileWriter( schema, null, out.getChannel() );
        writer.start();
      }
      writer.writeBatch();
    }
    writer.end();
    writer.close();
    reader.close();

    ArrowFileReader  ar = new ArrowFileReader( new FileInputStream( "target/array_column_test.arrow" ).getChannel() ,  new RootAllocator( Integer.MAX_VALUE ) );
    VectorSchemaRoot root = ar.getVectorSchemaRoot();
    ArrowBlock rbBlock = ar.getRecordBlocks().get(0);
    ar.loadRecordBatch(rbBlock);
    List<FieldVector> fieldVectorList = root.getFieldVectors();
    Spread spread = ArrowSpreadUtil.toSpread( root.getRowCount() , fieldVectorList );

    IColumn arrayColumn = spread.getColumn( "a" );
    assertEquals( arrayColumn.size() , 4 );

    IColumn childArrayColumn = arrayColumn.getColumn(0);
    ArrayCell cell0 = (ArrayCell)( arrayColumn.get(0) );
    assertEquals( cell0.getStart() , 0 );
    assertEquals( cell0.getEnd() , 2 );
    ArrayCell cell3 = (ArrayCell)( arrayColumn.get(3) );
    assertEquals( cell3.getStart() , 2 );
    assertEquals( cell3.getEnd() , 4 );

    assertEquals( ( (PrimitiveObject)( childArrayColumn.get(0).getRow() ) ).getInt() , 0 );
    assertEquals( ( (PrimitiveObject)( childArrayColumn.get(1).getRow() ) ).getInt() , 1 );
    assertEquals( ( (PrimitiveObject)( childArrayColumn.get(2).getRow() ) ).getInt() , 2 );
    assertEquals( ( (PrimitiveObject)( childArrayColumn.get(3).getRow() ) ).getInt() , 3 );

    file.delete();
  }

}
