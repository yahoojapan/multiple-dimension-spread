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
package jp.co.yahoo.dataplatform.mds.hadoop.hive.io;

import java.io.*;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import jp.co.yahoo.dataplatform.mds.MDSRecordWriter;
import jp.co.yahoo.dataplatform.mds.MDSWriter;
import jp.co.yahoo.dataplatform.mds.hadoop.hive.io.vector.ColumnVectorAssignorFactory;
import jp.co.yahoo.dataplatform.mds.hadoop.hive.io.vector.IColumnVectorAssignor;
import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.PerfectMatchStringFilter;
import jp.co.yahoo.dataplatform.mds.spread.expression.ExecuterNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.OrExpressionNode;
import jp.co.yahoo.dataplatform.mds.spread.expression.StringExtractNode;
import org.testng.annotations.Test;

import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.hive.ql.exec.vector.*;

import jp.co.yahoo.dataplatform.config.Configuration;
import jp.co.yahoo.dataplatform.schema.objects.*;

import jp.co.yahoo.dataplatform.mds.*;

public class TestMDSHiveDirectVectorizedReader{

  private void createFile( final String path )throws IOException{
    OutputStream out = new FileOutputStream( path );
    Configuration config = new jp.co.yahoo.dataplatform.config.Configuration();
    MDSRecordWriter writer = new MDSRecordWriter( out , config );

    Map<String,Object> dataContainer = new HashMap<String,Object>();

    for( int i = 0 ; i < 3000 ; i++ ){
      dataContainer.put( "str" , new StringObj( "a-" + i ) );
      dataContainer.put( "num" , new IntegerObj( i ) );
      dataContainer.put( "num2" , new IntegerObj( i * 2 ) );
      writer.addRow( dataContainer );
    }
    writer.close();
  }

  private void createFile2( final String path )throws IOException{
    OutputStream out = new FileOutputStream( path );
    Configuration config = new jp.co.yahoo.dataplatform.config.Configuration();
    MDSWriter writer = new MDSWriter( out , config );

    Map<String,Object> dataContainer = new HashMap<String,Object>();

    Spread s = new Spread();
    for( int i = 0 ; i < 3000 ; i++ ){
      dataContainer.put( "str" , new StringObj( "a-" + i ) );
      dataContainer.put( "num" , new IntegerObj( i ) );
      dataContainer.put( "num2" , new IntegerObj( i * 2 ) );
      s.addRow( dataContainer );
      if( ( i % 500 ) == 499 ){
        writer.append( s );
        s = new Spread();
      }
    }
    writer.close();
  }


  private HiveVectorizedReaderSetting getHiveVectorizedReaderSetting( final HiveReaderSetting setting ){
    StructTypeInfo info = new StructTypeInfo();
    ArrayList<String> nameList = new ArrayList<String>();
    nameList.add( "str" );
    nameList.add( "num" );
    nameList.add( "num2" );
    nameList.add( "n" );
    nameList.add( "p" );

    ArrayList<TypeInfo> typeInfoList = new ArrayList<TypeInfo>();
    typeInfoList.add( TypeInfoFactory.stringTypeInfo );
    typeInfoList.add( TypeInfoFactory.intTypeInfo );
    typeInfoList.add( TypeInfoFactory.intTypeInfo );
    typeInfoList.add( TypeInfoFactory.stringTypeInfo );
    typeInfoList.add( TypeInfoFactory.intTypeInfo );

    info.setAllStructFieldNames( nameList );
    info.setAllStructFieldTypeInfos( typeInfoList );

    IColumnVectorAssignor[] assignors = new IColumnVectorAssignor[typeInfoList.size()];
    for( int i = 0 ; i < assignors.length ; i++ ){
      assignors[i] = ColumnVectorAssignorFactory.create( typeInfoList.get(i) );
    }

    VectorizedRowBatchCtx rbCtx = new VectorizedRowBatchCtx( nameList.toArray( new String[0] ) , typeInfoList.toArray( new TypeInfo[0] ) , 1 , new String[0] );
    int[] needColumnIds = new int[]{ 0 , 2 , 4 };
    String[] columnNames = nameList.toArray( new String[0] );

    Object[] partitionValue = new Object[]{ 100 };
    return new HiveVectorizedReaderSetting( new boolean[]{ true , false , true , false , true } , partitionValue , rbCtx , assignors , needColumnIds , columnNames , setting );
  }

  @Test
  public void T_allTest_1() throws IOException{
    String dirName = this.getClass().getClassLoader().getResource( "io/out" ).getPath();
    String outPath = String.format( "%s/TestMDSHiveDirectVectorizedReader_T_allTest_1.mds" , dirName );
    createFile( outPath );

    HiveVectorizedReaderSetting setting = getHiveVectorizedReaderSetting( new HiveReaderSetting( new Configuration() , new OrExpressionNode() , true , false , false ) );
    File inFile = new File( outPath );
    MDSHiveDirectVectorizedReader reader = new MDSHiveDirectVectorizedReader( new FileInputStream( inFile ) , inFile.length() , 0 , inFile.length() , setting , new DummyJobReporter() );
    NullWritable key = reader.createKey();
    VectorizedRowBatch value = reader.createValue();
    int colCount = 0;
    while( reader.next( key , value ) ){
      BytesColumnVector str = (BytesColumnVector)value.cols[0];
      LongColumnVector num2 = (LongColumnVector)value.cols[2];
      LongColumnVector p = (LongColumnVector)value.cols[4];
      assertEquals( null , value.cols[1] );
      assertEquals( null , value.cols[3] );
      for( int i = 0 ; i < value.size ; i++,colCount++ ){
        assertEquals( new String( str.vector[i] , str.start[i] , str.length[i] ) , "a-" + colCount );
        assertEquals( num2.vector[i] , colCount * 2 );
        assertEquals( p.vector[0] , 100 );
      }
    }
    reader.getPos();
    reader.getProgress();
    reader.close();
  }

  @Test
  public void T_allTest_2() throws IOException{
    String dirName = this.getClass().getClassLoader().getResource( "io/out" ).getPath();
    String outPath = String.format( "%s/TestMDSHiveDirectVectorizedReader_T_allTest_2.mds" , dirName );
    createFile2( outPath );

    HiveVectorizedReaderSetting setting = getHiveVectorizedReaderSetting( new HiveReaderSetting( new Configuration() , new OrExpressionNode() , true , false , false ) );
    File inFile = new File( outPath );
    MDSHiveDirectVectorizedReader reader = new MDSHiveDirectVectorizedReader( new FileInputStream( inFile ) , inFile.length() , 0 , inFile.length() , setting , new DummyJobReporter() );
    NullWritable key = reader.createKey();
    VectorizedRowBatch value = reader.createValue();
    int colCount = 0;
    while( reader.next( key , value ) ){
      BytesColumnVector str = (BytesColumnVector)value.cols[0];
      LongColumnVector num2 = (LongColumnVector)value.cols[2];
      LongColumnVector p = (LongColumnVector)value.cols[4];
      assertEquals( null , value.cols[1] );
      assertEquals( null , value.cols[3] );
      for( int i = 0 ; i < value.size ; i++,colCount++ ){
        assertEquals( new String( str.vector[i] , str.start[i] , str.length[i] ) , "a-" + colCount );
        assertEquals( num2.vector[i] , colCount * 2 );
        assertEquals( p.vector[0] , 100 );
      }
    }
    reader.getPos();
    reader.getProgress();
    reader.close();
  }

  @Test
  public void T_allTest_3() throws IOException{
    String dirName = this.getClass().getClassLoader().getResource( "io/out" ).getPath();
    String outPath = String.format( "%s/TestMDSHiveDirectVectorizedReader_T_allTest_3.mds" , dirName );
    createFile2( outPath );

    OrExpressionNode or = new OrExpressionNode();
    or.addChildNode( new ExecuterNode( new StringExtractNode( "str" ) , new PerfectMatchStringFilter( "a-0" ) ) );
    HiveVectorizedReaderSetting setting = getHiveVectorizedReaderSetting( new HiveReaderSetting( new Configuration() , or , true , false , false ) );
    File inFile = new File( outPath );
    MDSHiveDirectVectorizedReader reader = new MDSHiveDirectVectorizedReader( new FileInputStream( inFile ) , inFile.length() , 0 , inFile.length() , setting , new DummyJobReporter() );
    NullWritable key = reader.createKey();
    VectorizedRowBatch value = reader.createValue();
    int colCount = 0;
    while( reader.next( key , value ) ){
      BytesColumnVector str = (BytesColumnVector)value.cols[0];
      LongColumnVector num2 = (LongColumnVector)value.cols[2];
      LongColumnVector p = (LongColumnVector)value.cols[4];
      assertEquals( null , value.cols[1] );
      assertEquals( null , value.cols[3] );
      for( int i = 0 ; i < value.size ; i++,colCount++ ){
        assertEquals( new String( str.vector[i] , str.start[i] , str.length[i] ) , "a-" + colCount );
        assertEquals( num2.vector[i] , colCount * 2 );
        assertEquals( p.vector[0] , 100 );
      }
    }
    reader.getPos();
    reader.getProgress();
    reader.close();
  }

}
