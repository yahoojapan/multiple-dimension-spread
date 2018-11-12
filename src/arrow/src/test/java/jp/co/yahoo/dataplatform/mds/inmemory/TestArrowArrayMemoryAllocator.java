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
package jp.co.yahoo.dataplatform.mds.inmemory;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;

import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.SchemaChangeCallBack;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.reader.*;
import org.apache.arrow.vector.complex.reader.BaseReader.*;
import org.apache.arrow.vector.complex.impl.*;
import org.apache.arrow.vector.complex.writer.*;
import org.apache.arrow.vector.complex.writer.BaseWriter.*;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;

import jp.co.yahoo.dataplatform.schema.objects.*;

import jp.co.yahoo.dataplatform.mds.spread.column.*;
import jp.co.yahoo.dataplatform.mds.binary.*;
import jp.co.yahoo.dataplatform.mds.binary.maker.*;

public class TestArrowArrayMemoryAllocator{

  @Test
  public void T_setArray_1() throws IOException{
    IColumn column = new ArrayColumn( "array" );
    List<Object> value = new ArrayList<Object>();
    value.add( new StringObj( "a" ) );
    value.add( new StringObj( "b" ) );
    value.add( new StringObj( "c" ) );
    column.add( ColumnType.ARRAY , value , 0 );
    column.add( ColumnType.ARRAY , value , 1 );
    column.add( ColumnType.ARRAY , value , 2 );
    column.add( ColumnType.ARRAY , value , 3 );
    column.add( ColumnType.ARRAY , value , 6 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new DumpArrayColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column );

    BufferAllocator allocator = new RootAllocator( 1024 * 1024 * 10 );
    SchemaChangeCallBack callBack = new SchemaChangeCallBack();
    StructVector parent = new StructVector("root", allocator, new FieldType(false, Struct.INSTANCE, null, null), callBack);
    parent.allocateNew();

    ListVector listVector = parent.addOrGetList( "target" );
    IMemoryAllocator memoryAllocator = new ArrowArrayMemoryAllocator( allocator , listVector );
    maker.loadInMemoryStorage( columnBinary , memoryAllocator );

    for( int i = 0 ; i < 7 ; i++ ){
      System.out.println( "count:" + listVector.getInnerValueCountAt(i) );
      System.out.println( "obj:" + listVector.getObject(i) );
    }
/*
    reader.setPosition( 0 );
    assertEquals( reader.read().booleanValue() , true );
    reader.setPosition( 1 );
    assertEquals( reader.readBoolean().booleanValue() , false );
    reader.setPosition( 5 );
    assertEquals( reader.readBoolean().booleanValue() , true );
    reader.setPosition( 2 );
    assertEquals( reader.readBoolean() , null );
    reader.setPosition( 3 );
    assertEquals( reader.readBoolean() , null );
    reader.setPosition( 4 );
    assertEquals( reader.readBoolean() , null );
*/
  }

}
