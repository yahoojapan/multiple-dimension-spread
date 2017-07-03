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
package jp.co.yahoo.dataplatform.mds.stats;

import java.util.Map;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import org.testng.annotations.Test;
import org.testng.annotations.DataProvider;

import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

public class TestColumnStats {

  private ColumnStats createChildStats( final String childName ){
    ColumnStats columnStats = new ColumnStats( childName );
    columnStats.addSummaryStats( ColumnType.STRING , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) );
    return columnStats;
  }

  private ColumnStats createMargeTestStats(){
    ColumnStats stats = new ColumnStats( "root" );
    stats.addSummaryStats( ColumnType.STRING , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) );
    stats.addChild( "child" , createChildStats( "child" ) );
    return stats;
  }

  @DataProvider(name = "T_addSummaryStats_1")
  public Object[][] data() {
    return new Object[][] {
      { ColumnType.STRING , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) }, 
      { ColumnType.INTEGER , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) }, 
      { ColumnType.UNION , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) }, 
      { ColumnType.ARRAY , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) }, 
      { ColumnType.SPREAD , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) }, 
      { ColumnType.BOOLEAN , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) }, 
      { ColumnType.BYTE , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) }, 
      { ColumnType.BYTES , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) }, 
      { ColumnType.DOUBLE , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) }, 
      { ColumnType.FLOAT , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) }, 
      { ColumnType.LONG , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) }, 
      { ColumnType.SHORT , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) }, 
      { ColumnType.NULL , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) }, 
      { ColumnType.EMPTY_ARRAY , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) }, 
      { ColumnType.EMPTY_SPREAD , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) }, 
      { ColumnType.UNKNOWN , new SummaryStats( 10 , 100 , 50 , 100 , 10 ) }, 
    };
  }

  @Test
  public void T_newInstance_1(){
    ColumnStats stats = new ColumnStats( null );
  }

  @Test( dataProvider = "T_addSummaryStats_1")
  public void T_addSummaryStats_1( final ColumnType columnType , final SummaryStats summaryStats ){
    ColumnStats stats = new ColumnStats( "root" );
    stats.addSummaryStats( columnType , summaryStats );

    Map<ColumnType,SummaryStats> statsMap = stats.getSummaryStats();
    SummaryStats result = statsMap.get( columnType );
    assertEquals( result.getRowCount() , 10 );
    assertEquals( result.getRawDataSize() , 100 );
    assertEquals( result.getRealDataSize() , 50 );
  }

  @Test( dataProvider = "T_addSummaryStats_1")
  public void T_margeSummaryStats_1( final ColumnType columnType , final SummaryStats summaryStats ){
    ColumnStats stats = new ColumnStats( "root" );
    stats.addSummaryStats( columnType , summaryStats );
    stats.margeSummaryStats( columnType , summaryStats );

    Map<ColumnType,SummaryStats> statsMap = stats.getSummaryStats();
    SummaryStats result = statsMap.get( columnType );
    assertEquals( result.getRowCount() , 20 );
    assertEquals( result.getRawDataSize() , 200 );
    assertEquals( result.getRealDataSize() , 100 );
  }

  @Test( dataProvider = "T_addSummaryStats_1")
  public void T_margeSummaryStats_2( final ColumnType columnType , final SummaryStats summaryStats ){
    ColumnStats stats = new ColumnStats( "root" );
    stats.margeSummaryStats( columnType , summaryStats );

    Map<ColumnType,SummaryStats> statsMap = stats.getSummaryStats();
    SummaryStats result = statsMap.get( columnType );
    assertEquals( result.getRowCount() , 10 );
    assertEquals( result.getRawDataSize() , 100 );
    assertEquals( result.getRealDataSize() , 50 );
  }

  @Test
  public void T_addChild_1(){
    ColumnStats stats = new ColumnStats( "root" );
    stats.addChild( "child" , createChildStats( "child" ) );

    Map<String,ColumnStats> childMap = stats.getChildColumnStats();
    ColumnStats childStats = childMap.get( "child" );
    assertEquals( childMap.size() , 1 );
    assertEquals( childStats != null , true );
    System.out.println( childStats.toString() );
  }

  @Test
  public void T_marge_1(){
    ColumnStats stats = createMargeTestStats();
    stats.marge( createMargeTestStats() );

    Map<ColumnType,SummaryStats> statsMap = stats.getSummaryStats();
    SummaryStats result = statsMap.get( ColumnType.STRING );
    assertEquals( result.getRowCount() , 20 );
    assertEquals( result.getRawDataSize() , 200 );
    assertEquals( result.getRealDataSize() , 100 );

    Map<String,ColumnStats> childMap = stats.getChildColumnStats();
    ColumnStats childStats = childMap.get( "child" );
    assertEquals( childMap.size() , 1 );
    assertEquals( childStats != null , true );
    Map<ColumnType,SummaryStats> childStatsMap = childStats.getSummaryStats();
    result = childStatsMap.get( ColumnType.STRING );
    assertEquals( result.getRowCount() , 20 );
    assertEquals( result.getRawDataSize() , 200 );
    assertEquals( result.getRealDataSize() , 100 );
  }

  @Test
  public void T_marge_2(){
    ColumnStats stats = new ColumnStats( "new" );
    stats.marge( createMargeTestStats() );

    Map<ColumnType,SummaryStats> statsMap = stats.getSummaryStats();
    SummaryStats result = statsMap.get( ColumnType.STRING );
    assertEquals( result.getRowCount() , 10 );
    assertEquals( result.getRawDataSize() , 100 );
    assertEquals( result.getRealDataSize() , 50 );

    Map<String,ColumnStats> childMap = stats.getChildColumnStats();
    ColumnStats childStats = childMap.get( "child" );
    assertEquals( childMap.size() , 1 );
    assertEquals( childStats != null , true );
    Map<ColumnType,SummaryStats> childStatsMap = childStats.getSummaryStats();
    result = childStatsMap.get( ColumnType.STRING );
    assertEquals( result.getRowCount() , 10 );
    assertEquals( result.getRawDataSize() , 100 );
    assertEquals( result.getRealDataSize() , 50 );
  }

  @Test
  public void T_doIntegration_1(){
    ColumnStats stats = createMargeTestStats();
    stats.marge( createMargeTestStats() );
    SummaryStats total = stats.doIntegration();

    assertEquals( total.getRowCount() , 40 );
    assertEquals( total.getRawDataSize() , 400 );
    assertEquals( total.getRealDataSize() , 200 );
  }

  @Test
  public void T_toJavaObject_1(){
    ColumnStats stats = createMargeTestStats();
    stats.marge( createMargeTestStats() );
    SummaryStats total = stats.doIntegration();
    Map<Object,Object> javaObj = stats.toJavaObject();
    assertEquals( javaObj.get( "name" ) , "root" );

    Map<Object,Object> totalMap = (Map<Object,Object>)( javaObj.get( "total" ) );
    assertEquals( totalMap.get( "field_count" ) , Long.valueOf( 20 ) );
    assertEquals( totalMap.get( "raw_data_size" ) , Long.valueOf( 200 ) );
    assertEquals( totalMap.get( "real_data_size" ) , Long.valueOf( 100 ) );

    
    Map<Object,Object> integMap = (Map<Object,Object>)( javaObj.get( "integratoin_total" ) );
    assertEquals( integMap.get( "field_count" ) , Long.valueOf( 40 ) );
    assertEquals( integMap.get( "raw_data_size" ) , Long.valueOf( 400 ) );
    assertEquals( integMap.get( "real_data_size" ) , Long.valueOf( 200 ) );

    List<Object> columnTypeList = (List<Object>)( javaObj.get( "column_types" ) );
    Map<Object,Object> columnTypeMap = (Map<Object,Object>)( columnTypeList.get( 0 ) );
    assertEquals( columnTypeMap.get( "field_count" ) , Long.valueOf( 20 ) );
    assertEquals( columnTypeMap.get( "raw_data_size" ) , Long.valueOf( 200 ) );
    assertEquals( columnTypeMap.get( "real_data_size" ) , Long.valueOf( 100 ) );

    Map<Object,Object> childContainerMap = (Map<Object,Object>)( javaObj.get( "child" ) );
    Map<Object,Object> childMap = (Map<Object,Object>)( childContainerMap.get( "child" ) );
    Map<Object,Object> childTotalMap = (Map<Object,Object>)( childMap.get( "total" ) );
    assertEquals( childTotalMap.get( "field_count" ) , Long.valueOf( 20 ) );
    assertEquals( childTotalMap.get( "raw_data_size" ) , Long.valueOf( 200 ) );
    assertEquals( childTotalMap.get( "real_data_size" ) , Long.valueOf( 100 ) );

    System.out.println( stats.toString() );
    System.out.println( javaObj.toString() );
  }


}
