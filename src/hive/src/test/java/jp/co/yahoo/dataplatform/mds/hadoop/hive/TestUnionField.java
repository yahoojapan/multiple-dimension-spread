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
package jp.co.yahoo.dataplatform.mds.hadoop.hive;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.dataplatform.mds.hadoop.hive.io.ColumnAndIndex;
import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.SpreadColumn;

import org.apache.hadoop.io.*;

import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import jp.co.yahoo.dataplatform.schema.objects.*;

public class TestUnionField{

  private UnionTypeInfo getTypeInfo(){
    UnionTypeInfo unionTypeInfo = new UnionTypeInfo();
    ArrayList<TypeInfo> uniTypeInfoList = new ArrayList<TypeInfo>();
    uniTypeInfoList.add( TypeInfoFactory.binaryTypeInfo  ); //0
    uniTypeInfoList.add( TypeInfoFactory.booleanTypeInfo  ); //1
    uniTypeInfoList.add( TypeInfoFactory.byteTypeInfo  ); //2
    uniTypeInfoList.add( TypeInfoFactory.doubleTypeInfo  ); //3
    uniTypeInfoList.add( TypeInfoFactory.floatTypeInfo  ); //4
    uniTypeInfoList.add( TypeInfoFactory.intTypeInfo ); //5
    uniTypeInfoList.add( TypeInfoFactory.longTypeInfo  ); //6
    uniTypeInfoList.add( TypeInfoFactory.shortTypeInfo  ); //7
    uniTypeInfoList.add( TypeInfoFactory.stringTypeInfo ); //8

    ListTypeInfo arrayInfo = new ListTypeInfo();
    arrayInfo.setListElementTypeInfo( TypeInfoFactory.stringTypeInfo );
    uniTypeInfoList.add( arrayInfo ); //9

    StructTypeInfo info = new StructTypeInfo();
    ArrayList<String> nameList = new ArrayList<String>();
    nameList.add( "str" );
    nameList.add( "num" );
    nameList.add( "arr" );
    nameList.add( "uni" );
    uniTypeInfoList.add( info ); //10

    uniTypeInfoList.add( new UnionTypeInfo() ); //11

    unionTypeInfo.setAllUnionObjectTypeInfos( uniTypeInfoList );
    return unionTypeInfo;
  }

  private UnionTypeInfo getUnsupportInfo(){
    UnionTypeInfo unionTypeInfo = new UnionTypeInfo();
    ArrayList<TypeInfo> uniTypeInfoList = new ArrayList<TypeInfo>();

    uniTypeInfoList.add( TypeInfoFactory.charTypeInfo  );
    uniTypeInfoList.add( TypeInfoFactory.dateTypeInfo  );
    uniTypeInfoList.add( TypeInfoFactory.decimalTypeInfo  );
    uniTypeInfoList.add( TypeInfoFactory.timestampTypeInfo  );
    uniTypeInfoList.add( TypeInfoFactory.unknownTypeInfo  );
    uniTypeInfoList.add( TypeInfoFactory.varcharTypeInfo  );
    uniTypeInfoList.add( TypeInfoFactory.voidTypeInfo  );

    unionTypeInfo.setAllUnionObjectTypeInfos( uniTypeInfoList );

    return unionTypeInfo;
  }

  @Test
  public void T_newInstance_1(){
    UnionTypeInfo unionTypeInfo = getTypeInfo();
    UnionField unionField = new UnionField( unionTypeInfo );
  }

  @Test
  public void T_bytes_1() throws IOException{
    UnionTypeInfo unionTypeInfo = getTypeInfo();
    UnionField unionField = new UnionField( unionTypeInfo );

    IColumn column = new PrimitiveColumn( ColumnType.BYTES , "hoge" );
    byte[] inBytes = "f1".getBytes( "UTF8" );
    column.add( ColumnType.BYTES , new BytesObj( inBytes ) , 0 );

    ColumnAndIndex columnAndIndex = new ColumnAndIndex( column , 0 , 0 );

    UnionObject union = unionField.get( columnAndIndex );

    assertEquals( union.getTag() , (byte)0 );
    assertEquals( union.getObject() , new BytesWritable( inBytes ) );
  }

  @Test
  public void T_boolean_1() throws IOException{
    UnionTypeInfo unionTypeInfo = getTypeInfo();
    UnionField unionField = new UnionField( unionTypeInfo );

    IColumn column = new PrimitiveColumn( ColumnType.BOOLEAN , "hoge" );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 0 );

    ColumnAndIndex columnAndIndex = new ColumnAndIndex( column , 0 , 0 );

    UnionObject union = unionField.get( columnAndIndex );

    assertEquals( union.getTag() , (byte)1 );
    assertEquals( union.getObject() , new BooleanWritable( true ) );
  }

  @Test
  public void T_byte_1() throws IOException{
    UnionTypeInfo unionTypeInfo = getTypeInfo();
    UnionField unionField = new UnionField( unionTypeInfo );

    IColumn column = new PrimitiveColumn( ColumnType.BYTE , "hoge" );
    column.add( ColumnType.BYTE , new ByteObj( (byte)255 ) , 0 );

    ColumnAndIndex columnAndIndex = new ColumnAndIndex( column , 0 , 0 );

    UnionObject union = unionField.get( columnAndIndex );

    assertEquals( union.getTag() , (byte)2 );
    assertEquals( union.getObject() , new ByteWritable( (byte)255 ) );
  }

  @Test
  public void T_double_1() throws IOException{
    UnionTypeInfo unionTypeInfo = getTypeInfo();
    UnionField unionField = new UnionField( unionTypeInfo );

    IColumn column = new PrimitiveColumn( ColumnType.DOUBLE , "hoge" );
    column.add( ColumnType.DOUBLE , new DoubleObj( (double)0.1 ) , 0 );

    ColumnAndIndex columnAndIndex = new ColumnAndIndex( column , 0 , 0 );

    UnionObject union = unionField.get( columnAndIndex );

    assertEquals( union.getTag() , (byte)3 );
    assertEquals( union.getObject() , new DoubleWritable( (double)0.1 ) );
  }

  @Test
  public void T_float_1() throws IOException{
    UnionTypeInfo unionTypeInfo = getTypeInfo();
    UnionField unionField = new UnionField( unionTypeInfo );

    IColumn column = new PrimitiveColumn( ColumnType.FLOAT , "hoge" );
    column.add( ColumnType.FLOAT , new FloatObj( (float)0.1 ) , 0 );

    ColumnAndIndex columnAndIndex = new ColumnAndIndex( column , 0 , 0 );

    UnionObject union = unionField.get( columnAndIndex );

    assertEquals( union.getTag() , (byte)4 );
    assertEquals( union.getObject() , new FloatWritable( (float)0.1 ) );
  }

  @Test
  public void T_int_1() throws IOException{
    UnionTypeInfo unionTypeInfo = getTypeInfo();
    UnionField unionField = new UnionField( unionTypeInfo );

    IColumn column = new PrimitiveColumn( ColumnType.INTEGER , "hoge" );
    column.add( ColumnType.INTEGER , new IntegerObj( 1 ) , 0 );

    ColumnAndIndex columnAndIndex = new ColumnAndIndex( column , 0 , 0 );

    UnionObject union = unionField.get( columnAndIndex );

    assertEquals( union.getTag() , (byte)5 );
    assertEquals( union.getObject() , new IntWritable( 1 ) );
  }

  @Test
  public void T_long_1() throws IOException{
    UnionTypeInfo unionTypeInfo = getTypeInfo();
    UnionField unionField = new UnionField( unionTypeInfo );

    IColumn column = new PrimitiveColumn( ColumnType.LONG , "hoge" );
    column.add( ColumnType.LONG , new LongObj( 1 ) , 0 );

    ColumnAndIndex columnAndIndex = new ColumnAndIndex( column , 0 , 0 );

    UnionObject union = unionField.get( columnAndIndex );

    assertEquals( union.getTag() , (byte)6 );
    assertEquals( union.getObject() , new LongWritable( 1 ) );
  }

  @Test
  public void T_short_1() throws IOException{
    UnionTypeInfo unionTypeInfo = getTypeInfo();
    UnionField unionField = new UnionField( unionTypeInfo );

    IColumn column = new PrimitiveColumn( ColumnType.SHORT , "hoge" );
    column.add( ColumnType.SHORT , new ShortObj( (short)1 ) , 0 );

    ColumnAndIndex columnAndIndex = new ColumnAndIndex( column , 0 , 0 );

    UnionObject union = unionField.get( columnAndIndex );

    assertEquals( union.getTag() , (byte)7 );
    assertEquals( union.getObject() , new org.apache.hadoop.hive.serde2.io.ShortWritable( (short)1 ) );
  }

  @Test
  public void T_null_1() throws IOException{
    UnionTypeInfo unionTypeInfo = getTypeInfo();
    UnionField unionField = new UnionField( unionTypeInfo );

    IColumn column = new PrimitiveColumn( ColumnType.SHORT , "hoge" );

    ColumnAndIndex columnAndIndex = new ColumnAndIndex( column , 0 , 0 );

    UnionObject union = unionField.get( columnAndIndex );

    assertEquals( null , union );
  }

  @Test
  public void T_string_1() throws IOException{
    UnionTypeInfo unionTypeInfo = getTypeInfo();
    UnionField unionField = new UnionField( unionTypeInfo );

    IColumn column = new PrimitiveColumn( ColumnType.STRING , "hoge" );
    column.add( ColumnType.STRING , new StringObj( "f1" ) , 0 );

    ColumnAndIndex columnAndIndex = new ColumnAndIndex( column , 0 , 0 );

    UnionObject union = unionField.get( columnAndIndex );

    assertEquals( union.getTag() , (byte)8 );
    assertEquals( union.getObject() , new Text( "f1" ) );
  }

  @Test
  public void T_struct_1() throws IOException{
    UnionTypeInfo unionTypeInfo = getTypeInfo();
    UnionField unionField = new UnionField( unionTypeInfo );

    Map<String,Object> dataContainer = new HashMap<String,Object>();
    dataContainer.put( "str" , new StringObj( "a" ) );
    dataContainer.put( "num" , new IntegerObj( 1 ) );
    dataContainer.put( "uni" , new IntegerObj( 1 ) );

    Spread spread = new Spread();
    spread.addRow( dataContainer );
    SpreadColumn spreadColumn = new SpreadColumn( "root" );
    spreadColumn.setSpread( spread );
    ColumnAndIndex columnAndIndex = new ColumnAndIndex( spreadColumn , 0 , 0 );

    UnionObject union = unionField.get( columnAndIndex );

    assertEquals( union.getTag() , (byte)10 );
    assertEquals( union.getObject().getClass().getName() , ColumnAndIndex.class.getName() );
  }

  @Test
  public void T_array_1() throws IOException{
    UnionTypeInfo unionTypeInfo = getTypeInfo();
    UnionField unionField = new UnionField( unionTypeInfo );

    Map<String,Object> dataContainer = new HashMap<String,Object>();
    List<Object> array = new ArrayList<Object>();
    array.add( new StringObj( "a" ) );
    array.add( new StringObj( "b" ) );
    array.add( new StringObj( "c" ) );
    dataContainer.put( "array" , array );
    Spread spread = new Spread();
    spread.addRow( dataContainer );

    ColumnAndIndex columnAndIndex = new ColumnAndIndex( spread.getColumn( "array" ) , 0 , 0 );

    UnionObject union = unionField.get( columnAndIndex );

    assertEquals( union.getTag() , (byte)9 );
    assertEquals( union.getObject().getClass().getName() , ColumnAndIndex.class.getName() );
  }

}
