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
package jp.co.yahoo.dataplatform.mds.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;

import jp.co.yahoo.dataplatform.config.Configuration;

import jp.co.yahoo.dataplatform.schema.objects.*;

import jp.co.yahoo.dataplatform.mds.MDSWriter;
import jp.co.yahoo.dataplatform.mds.MDSReader;
import jp.co.yahoo.dataplatform.mds.MDSArrowReader;
import jp.co.yahoo.dataplatform.mds.spread.Spread;

public class TestConvertArrowFormatTool{

  private byte[] createTestData() throws IOException{
    // Spread
    // | col1 | col2 | col3 |
    // | 100  | aaa  | NULL |
    // | 200  | NULL | BBB  |
    // | 300  | NULL | CCC  |
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    Spread s = new Spread();
    try (MDSWriter writer = new MDSWriter(out, config)) {
      Map<String, Object> d = new HashMap<String, Object>();
      d.put( "col1" , new LongObj( 100 ) );
      d.put( "col2" , new StringObj( "aaa" ) );
      s.addRow( d );

      d.clear();
      d.put( "col1" , new LongObj( 200 ) );
      d.put( "col3" , new StringObj( "BBB" ) );
      s.addRow( d );

      d.clear();
      d.put( "col1" , new LongObj( 300 ) );
      d.put( "col3" , new StringObj( "CCC" ) );
      s.addRow( d );
      writer.append(s);
    }
    return out.toByteArray();
  }

  @Test
  public void T_convert_1() throws IOException{
    byte[] mdsFile = createTestData();
    InputStream in = new ByteArrayInputStream( mdsFile );
    MDSReader reader = new MDSReader();
    Configuration config = new Configuration();
    reader.setNewStream( in , mdsFile.length , config );
    MDSArrowReader arrowReader = new MDSArrowReader( reader , config );
    File testFile = new File( "target/TestConvertArrowFormatTool_T_convert_1.mds" );
    if( testFile.exists() ){
      testFile.delete();
    }
    FileOutputStream out = new FileOutputStream( testFile );
    ConvertArrowFormatTool.convert( arrowReader , out , config );

    FileInputStream arrowIn = new FileInputStream( testFile ); 
    ArrowFileReader ar = new ArrowFileReader( arrowIn.getChannel() , new RootAllocator( Integer.MAX_VALUE ) );
    VectorSchemaRoot root  = ar.getVectorSchemaRoot();
    ArrowBlock rbBlock = ar.getRecordBlocks().get(0);
    ar.loadRecordBatch(rbBlock);
    List<FieldVector> fieldVectorList = root.getFieldVectors();
    Map<String,FieldVector> vectorMap = new HashMap<String,FieldVector>();
    for( FieldVector v : fieldVectorList ){
      vectorMap.put( v.getField().getName() , v );
    }

    assertTrue( vectorMap.containsKey( "col1" ) );
    assertTrue( vectorMap.containsKey( "col2" ) );
    assertTrue( vectorMap.containsKey( "col3" ) );

    BigIntVector col1 = (BigIntVector)( vectorMap.get( "col1" ) );
    VarCharVector col2 = (VarCharVector)( vectorMap.get( "col2" ) );
    VarCharVector col3 = (VarCharVector)( vectorMap.get( "col3" ) );


    assertEquals( col1.get(0) , 100L );
    assertEquals( col1.get(1) , 200L );
    assertEquals( col1.get(2) , 300L );

    assertEquals( col2.getObject(0).toString() , "aaa" );
    assertTrue( col2.isNull(1) );
    assertTrue( col2.isNull(2) );

    assertTrue( col3.isNull(0) );
    assertEquals( col3.getObject(1).toString() , "BBB" );
    assertEquals( col3.getObject(2).toString() , "CCC" );

    testFile.delete();
    ar.close();
  }

}
