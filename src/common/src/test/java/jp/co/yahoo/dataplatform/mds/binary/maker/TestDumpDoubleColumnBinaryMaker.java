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
package jp.co.yahoo.dataplatform.mds.binary.maker;

import java.io.IOException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import org.testng.annotations.Test;

import jp.co.yahoo.dataplatform.schema.objects.DoubleObj;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

public class TestDumpDoubleColumnBinaryMaker {

  @Test
  public void T_toBinary_1() throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.DOUBLE , "DOUBLE" );
    column.add( ColumnType.DOUBLE , new DoubleObj( (double)0.1 ) , 0 );
    column.add( ColumnType.DOUBLE , new DoubleObj( (double)0.2 ) , 1 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new DumpDoubleColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column , new MakerCache() );

    assertEquals( columnBinary.columnName , "DOUBLE" );
    assertEquals( columnBinary.rowCount , 2 );
    assertEquals( columnBinary.columnType , ColumnType.DOUBLE );

    IColumn decodeColumn = maker.toColumn( columnBinary , new DefaultPrimitiveObjectConnector() );
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );

    assertEquals( (double)0.1 , ( (PrimitiveObject)( decodeColumn.get(0).getRow() ) ).getDouble() );
    assertEquals( (double)0.2 , ( (PrimitiveObject)( decodeColumn.get(1).getRow() ) ).getDouble() );

    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 0 );
  }
}

