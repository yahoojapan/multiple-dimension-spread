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
package jp.co.yahoo.dataplatform.mds.hadoop.hive.io.vector;

import java.io.*;

import java.util.Arrays;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.dataplatform.mds.binary.UTF8BytesLinkObj;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import jp.co.yahoo.dataplatform.mds.spread.expression.AllExpressionIndex;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionIndex;

import org.apache.hadoop.hive.ql.exec.vector.*;

import jp.co.yahoo.dataplatform.schema.objects.*;

import jp.co.yahoo.dataplatform.mds.*;

public class TestBytesColumnVectorAssignor{

  @Test
  public void T_set_1() throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BYTES , "t" );
    for( int i = 0 ; i < 2000 ; i++ ){
      column.add( ColumnType.BYTES , new BytesObj( Integer.toString( i ).getBytes() ) , i );
    }
    IExpressionIndex index = new AllExpressionIndex( column.size() );

    BytesColumnVector vector = new BytesColumnVector( 1024 );
    IColumnVectorAssignor assignor = new BytesColumnVectorAssignor();
    assignor.setColumn( column.size() , column );

    for( int i = 0 ; i < 3 ; i++ ){
      int start = i * 1024;
      assignor.setColumnVector( vector , index , start , 1024 );
      for( int n = 0 ; n < 1024 ; n++ ){
        if( ( n + start ) < 2000 ){
          assertTrue( Arrays.equals( vector.vector[n] , Integer.toString( n + start ).getBytes() ) );
        }
        else{
          assertTrue( vector.isNull[n] );
        }
      }
    }
  }

  @Test
  public void T_set_2() throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BYTES , "t" );
    for( int i = 0 ; i < 2000 ; i++ ){
      byte[] a = Integer.toString( i ).getBytes();
      column.add( ColumnType.BYTES , new UTF8BytesLinkObj( a , 0 , a.length ) , i );
    }
    IExpressionIndex index = new AllExpressionIndex( column.size() );

    BytesColumnVector vector = new BytesColumnVector( 1024 );
    IColumnVectorAssignor assignor = new BytesColumnVectorAssignor();
    assignor.setColumn( column.size() , column );

    for( int i = 0 ; i < 3 ; i++ ){
      int start = i * 1024;
      assignor.setColumnVector( vector , index , start , 1024 );
      for( int n = 0 ; n < 1024 ; n++ ){
        if( ( n + start ) < 2000 ){
          assertTrue( Arrays.equals( vector.vector[n] , Integer.toString( n + start ).getBytes() ) );
        }
        else{
          assertTrue( vector.isNull[n] );
        }
      }
    }
  }

}
