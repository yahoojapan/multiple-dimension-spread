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

import java.util.List;
import java.util.ArrayList;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.spread.column.ArrayColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import org.testng.Assert;
import org.testng.annotations.Test;

import jp.co.yahoo.dataplatform.schema.objects.*;

public class TestDumpArrayColumnBinaryMaker{

  @Test
  public void T_toBinary_1() throws IOException{
    IColumn column = new ArrayColumn( "array" );
    List<Object> value = new ArrayList<Object>();
    value.add( new StringObj( "a" ) );
    value.add( new StringObj( "b" ) );
    value.add( new StringObj( "c" ) );
    column.add( ColumnType.ARRAY , value , 0 );
    column.add( ColumnType.ARRAY , value , 1 );
    column.add( ColumnType.ARRAY , value , 2 );
    column.add( ColumnType.ARRAY , value , 3 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new DumpArrayColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , column , new MakerCache() );

    assertEquals( columnBinary.columnName , "array" );
    assertEquals( columnBinary.rowCount , 4 );
    Assert.assertEquals( columnBinary.columnType , ColumnType.ARRAY );

    IColumn decodeColumn = maker.toColumn( columnBinary , new DefaultPrimitiveObjectConnector() );
    IColumn expandColumn = decodeColumn.getColumn(0);
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 1 );
    for( int i = 0 ; i < 3 * 4 ; i+=3 ){
      assertEquals( ( (PrimitiveObject)( expandColumn.get(i).getRow() ) ).getString() , "a" );
      assertEquals( ( (PrimitiveObject)( expandColumn.get(i+1).getRow() ) ).getString() , "b" );
      assertEquals( ( (PrimitiveObject)( expandColumn.get(i+2).getRow() ) ).getString() , "c" );
    }
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 1 );
  }

}
