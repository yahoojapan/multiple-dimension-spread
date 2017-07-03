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
package jp.co.yahoo.dataplatform.mds.example.spread;

import java.io.IOException;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;

import jp.co.yahoo.dataplatform.schema.objects.*;

import jp.co.yahoo.dataplatform.mds.spread.Spread;

public final class UseSpreadStep4{

  private UseSpreadStep4(){}

  public int run() throws IOException{
    Spread spread = new Spread();

    System.out.println( String.format( "Create empty Spread." ) );
    System.out.println( spread.toString() );

    Map<String,Object> dataContainer = new LinkedHashMap<String,Object>();
    Map<String,Object> summaryContainer = new LinkedHashMap<String,Object>();
    List<Object> weightList = new ArrayList<Object>();
    dataContainer.put( "name" , new StringObj( "apple" ) );
    dataContainer.put( "class" , new StringObj( "fruits" ) );
    dataContainer.put( "number" , new IntegerObj( 5 ) );
    dataContainer.put( "price" , new IntegerObj( 110 ) );

    summaryContainer.put( "total_price" , new IntegerObj( 550 ) );
    summaryContainer.put( "total_weight" , new IntegerObj( 412 ) );

    weightList.add( new IntegerObj( 64 ) );
    weightList.add( new IntegerObj( 72 ) );
    weightList.add( new IntegerObj( 74 ) );
    weightList.add( new IntegerObj( 112 ) );
    weightList.add( new IntegerObj( 90 ) );

    summaryContainer.put( "weight_array" , weightList );

    dataContainer.put( "summary" , summaryContainer );

    System.out.println( String.format( "Add new data. name:apple , class:fruits , number:5 , price:110 , summary:{ total_price:550 , weight:600 , weight_array:[ 64 , 72 , 74 , 112 , 90 ] }" ) );
    spread.addRow( dataContainer );

    System.out.println( String.format( "Spread dump." ) );
    System.out.println( spread.toString() );

    return 0;
  }

  public static void main( final String[] args ) throws IOException{
    new UseSpreadStep4().run();
  }

}
