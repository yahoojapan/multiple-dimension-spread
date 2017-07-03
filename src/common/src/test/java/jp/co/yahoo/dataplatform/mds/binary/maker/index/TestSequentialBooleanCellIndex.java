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
package jp.co.yahoo.dataplatform.mds.binary.maker.index;

import java.io.IOException;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;

import jp.co.yahoo.dataplatform.mds.spread.column.filter.BooleanFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.NullFilter;
import org.testng.annotations.Test;

public class TestSequentialBooleanCellIndex{

  @Test
  public void T_newInstance_1(){
    SequentialBooleanCellIndex index = new SequentialBooleanCellIndex( new byte[0] );
  }

  @Test
  public void T_filter_1() throws IOException{
    byte[] data = new byte[10];
    for( int i = 0 ; i < 10 ; i++ ){
      data[i] = (byte)( i % 2 );
    }
    SequentialBooleanCellIndex index = new SequentialBooleanCellIndex( data );
    List<Integer> result = index.filter( new BooleanFilter( true ) );
    assertEquals( result.size() , 5 );
    for( int i = 0 , n = 0 ; i < 10 ; i+=2,n++ ){
      assertEquals( result.get(n).intValue() , i+1 );
    }
  }

  @Test
  public void T_filter_2() throws IOException{
    byte[] data = new byte[10];
    for( int i = 0 ; i < 10 ; i++ ){
      data[i] = (byte)( i % 2 );
    }
    SequentialBooleanCellIndex index = new SequentialBooleanCellIndex( data );
    List<Integer> result = index.filter( new BooleanFilter( false ) );
    assertEquals( result.size() , 5 );
    for( int i = 0 , n = 0 ; i < 10 ; i+=2,n++ ){
      assertEquals( result.get(n).intValue() , i );
    }
  }

  @Test
  public void T_filter_3() throws IOException{
    byte[] data = new byte[10];
    for( int i = 0 ; i < 10 ; i++ ){
      data[i] = (byte)( i % 2 );
    }
    SequentialBooleanCellIndex index = new SequentialBooleanCellIndex( data );
    List<Integer> result = index.filter( null );
    assertEquals( result , null );
  }

  @Test
  public void T_filter_4() throws IOException{
    byte[] data = new byte[10];
    for( int i = 0 ; i < 10 ; i++ ){
      data[i] = (byte)( i % 2 );
    }
    SequentialBooleanCellIndex index = new SequentialBooleanCellIndex( data );
    List<Integer> result = index.filter( new NullFilter() );
    assertEquals( result , null );
  }

}
