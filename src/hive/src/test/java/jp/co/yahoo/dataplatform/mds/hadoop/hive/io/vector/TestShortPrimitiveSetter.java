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

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import jp.co.yahoo.dataplatform.mds.spread.expression.AllExpressionIndex;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionIndex;

import org.apache.hadoop.hive.ql.exec.vector.*;

import jp.co.yahoo.dataplatform.schema.objects.*;

import jp.co.yahoo.dataplatform.mds.*;

public class TestShortPrimitiveSetter{

  @Test
  public void T_set_1() throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.SHORT , "t" );
    for( int i = 0 ; i < 2000 ; i++ ){
      column.add( ColumnType.SHORT , new ShortObj( (short)( i % 128 ) ) , i );
    }
    IExpressionIndex index = new AllExpressionIndex( column.size() );

    INumberPrimitiveSetter setter = ShortPrimitiveSetter.getInstance();
    for( int i = 0 ; i < 3 ; i++ ){
      int start = i * 1024;
      PrimitiveObject[] pArray = column.getPrimitiveObjectArray( index , start , 1024 );
      LongColumnVector vector = new LongColumnVector( 1024 );
      for( int n = 0 ; n < 1024 ; n++ ){
        setter.set( pArray , vector , n );
      } 
      for( int n = 0 ; n < 1024 ; n++ ){
        if( ( n + start ) < 2000 ){
          assertEquals( vector.vector[n] , ( n + start ) % 128 );
        }
        else{
          assertTrue( vector.isNull[n] );
        }
      }
    }
  }

}
