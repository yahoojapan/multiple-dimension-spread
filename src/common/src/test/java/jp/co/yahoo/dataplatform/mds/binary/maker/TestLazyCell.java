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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import org.testng.annotations.Test;

import jp.co.yahoo.dataplatform.schema.objects.*;

import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

public class TestLazyCell {

  private class TestDicManager implements IDicManager{

    private final List<PrimitiveObject> dicList;

    public TestDicManager(){
      dicList = new ArrayList<PrimitiveObject>();
      dicList.add( null );
      dicList.add( new StringObj( "a" ) );
      dicList.add( new StringObj( "b" ) );
      dicList.add( new StringObj( "c" ) );
    }

    @Override
    public PrimitiveObject get( final int index ) throws IOException{
      return dicList.get( index );
    }

    @Override
    public int getDicSize() throws IOException{
      return dicList.size();
    }
  }

  private class TestExceptionDicManager implements IDicManager{

    @Override
    public PrimitiveObject get( final int index ) throws IOException{
      throw new IOException( "get" );
    }

    @Override
    public int getDicSize() throws IOException{
      throw new IOException( "getDicSize" );
    }
  }

  @Test
  public void T_newInstance_1() throws IOException{
    LazyCell cell1 = new LazyCell( ColumnType.STRING , new TestDicManager() , 1 );
    LazyCell cell2 = new LazyCell( ColumnType.STRING , new TestDicManager() , 2 );
    LazyCell cell3 = new LazyCell( ColumnType.STRING , new TestDicManager() , 3 );

    assertEquals( cell1.getRow().getString() , "a" );
    assertEquals( cell2.getRow().getString() , "b" );
    assertEquals( cell3.getRow().getString() , "c" );

    assertEquals( cell1.getType() , ColumnType.STRING );
    assertEquals( cell2.getType() , ColumnType.STRING );
    assertEquals( cell3.getType() , ColumnType.STRING );
    
    assertEquals( cell1.toString() , "a" );
    assertEquals( cell2.toString() , "b" );
    assertEquals( cell3.toString() , "c" );
  }

  @Test( expectedExceptions = { UnsupportedOperationException.class } )
  public void T_getRow(){
    LazyCell cell1 = new LazyCell( ColumnType.STRING , new TestExceptionDicManager() , 1 );
    cell1.getRow();
  }

  @Test( expectedExceptions = { UnsupportedOperationException.class } )
  public void T_setRow(){
    LazyCell cell1 = new LazyCell( ColumnType.STRING , new TestExceptionDicManager() , 1 );
    cell1.setRow( null );
  }
}
