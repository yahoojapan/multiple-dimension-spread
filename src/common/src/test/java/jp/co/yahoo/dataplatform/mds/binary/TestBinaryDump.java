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
package jp.co.yahoo.dataplatform.mds.binary;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import org.testng.annotations.Test;

public class TestBinaryDump{

  @Test
  public void T_dumpByte_1(){
    List<Byte> list = new ArrayList<Byte>();
    for( int i = 0 ; i < 256 ; i++ ){
      if( ( i % 3 ) == 0 ){
        list.add( Integer.valueOf( i ).byteValue() );
      }
    }
    byte[] binary = BinaryDump.dumpByte( list );
    List<Byte> decodeList = BinaryDump.binaryToByteList( binary , 0 , binary.length );
    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
    assertEquals( binary.length , list.size() );
  }

  @Test
  public void T_dumpByte_2(){
    List<Byte> list = new ArrayList<Byte>();
    byte[] binary = BinaryDump.dumpByte( list );
    List<Byte> decodeList = BinaryDump.binaryToByteList( binary , 0 , binary.length );
    assertEquals( list.size() , decodeList.size() );
    assertEquals( binary.length , 0 );
  }

  @Test
  public void T_dumpByte_3(){
    List<Byte> list = null;
    byte[] binary = BinaryDump.dumpByte( list );
    List<Byte> decodeList = BinaryDump.binaryToByteList( binary , 0 , binary.length );
    assertEquals( decodeList.size() , 0 );
    assertEquals( binary.length , 0 );
  }

  @Test
  public void T_dumpButes_1() throws IOException{
    List<byte[]> list = new ArrayList<byte[]>();
    list.add( new byte[0] );
    list.add( "abcd".getBytes( "UTF-8" ) );
    list.add( "あいうえお".getBytes( "UTF-8" ) );
    byte[] binary = BinaryDump.dumpBytes( list , 19 );
    List<byte[]> decodeList = BinaryDump.binaryToBytesList( binary , 0 , binary.length );
    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( Arrays.equals( list.get(i) , decodeList.get(i) ) , true );
    }
    assertEquals( 19 + 4 * 3 , binary.length );
  }

  @Test
  public void T_dumpBytes_2() throws IOException{
    List<byte[]> list = new ArrayList<byte[]>();
    byte[] binary = BinaryDump.dumpBytes( list , 0 );
    List<byte[]> decodeList = BinaryDump.binaryToBytesList( binary , 0 , binary.length );
    assertEquals( list.size() , decodeList.size() );
  }

  @Test( expectedExceptions = { ArrayIndexOutOfBoundsException.class } )
  public void T_dumpBytes_3() throws IOException{
    List<byte[]> list = new ArrayList<byte[]>();
    list.add( "abcd".getBytes( "UTF-8" ) );
    byte[] binary = BinaryDump.dumpBytes( list , 0 );
  }

  @Test( expectedExceptions = { NullPointerException.class } )
  public void T_dumpBytes_4() throws IOException{
    List<byte[]> list = new ArrayList<byte[]>();
    list.add( null );
    byte[] binary = BinaryDump.dumpBytes( list , 0 );
    List<byte[]> decodeList = BinaryDump.binaryToBytesList( binary , 0 , binary.length );
  }

  @Test
  public void T_dumpString_1() throws IOException{
    List<String> list = new ArrayList<String>();
    list.add( "" );
    list.add( "abcd" );
    list.add( "あいうえお" );
    byte[] binary = BinaryDump.dumpString( list , 9 * 2 );
    List<String> decodeList = BinaryDump.binaryToStringList( binary , 0 , binary.length );
    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
  }

  @Test
  public void T_dumpString_2() throws IOException{
    List<String> list = new ArrayList<String>();
    byte[] binary = BinaryDump.dumpString( list , 0 );
    List<String> decodeList = BinaryDump.binaryToStringList( binary , 0 , binary.length );
    assertEquals( list.size() , decodeList.size() );
  }

  @Test( expectedExceptions = { java.nio.BufferOverflowException.class } )
  public void T_dumpString_3() throws IOException{
    List<String> list = new ArrayList<String>();
    list.add( "abcd" );
    byte[] binary = BinaryDump.dumpString( list , 0 );
  }

  @Test( expectedExceptions = { NullPointerException.class } )
  public void T_dumpString_4() throws IOException{
    List<String> list = new ArrayList<String>();
    list.add( null );
    byte[] binary = BinaryDump.dumpString( list , 0 );
  }

  @Test
  public void T_dumpDouble_1(){
    List<Double> list = new ArrayList<Double>();
    for( int i = 3000 ; i < 5000 ; i++ ){
      if( ( i % 3 ) == 0 ){
        list.add( Integer.valueOf( i ).doubleValue() / (double)2 );
      }
    }
    byte[] binary = BinaryDump.dumpDouble( list );
    List<Double> decodeList = BinaryDump.binaryToDoubleList( binary , 0 , binary.length );
    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_dumpDouble_2(){
    List<Double> list = new ArrayList<Double>();
    byte[] binary = BinaryDump.dumpDouble( list );
    List<Double> decodeList = BinaryDump.binaryToDoubleList( binary , 0 , binary.length );
    assertEquals( list.size() , decodeList.size() );
    assertEquals( binary.length , 0 );
  }

  @Test
  public void T_dumpDouble_3(){
    List<Double> list = null;
    byte[] binary = BinaryDump.dumpDouble( list );
    List<Double> decodeList = BinaryDump.binaryToDoubleList( binary , 0 , binary.length );
    assertEquals( decodeList.size() , 0 );
    assertEquals( binary.length , 0 );
  }

  @Test
  public void T_dumpFloat_1(){
    List<Float> list = new ArrayList<Float>();
    for( int i = 3000 ; i < 5000 ; i++ ){
      if( ( i % 3 ) == 0 ){
        list.add( Integer.valueOf( i ).floatValue() / (float)2 );
      }
    }
    byte[] binary = BinaryDump.dumpFloat( list );
    List<Float> decodeList = BinaryDump.binaryToFloatList( binary , 0 , binary.length );
    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_dumpFloat_2(){
    List<Float> list = new ArrayList<Float>();
    byte[] binary = BinaryDump.dumpFloat( list );
    List<Float> decodeList = BinaryDump.binaryToFloatList( binary , 0 , binary.length );
    assertEquals( list.size() , decodeList.size() );
    assertEquals( binary.length , 0 );
  }

  @Test
  public void T_dumpFloat_3(){
    List<Float> list = null;
    byte[] binary = BinaryDump.dumpFloat( list );
    List<Float> decodeList = BinaryDump.binaryToFloatList( binary , 0 , binary.length );
    assertEquals( decodeList.size() , 0 );
    assertEquals( binary.length , 0 );
  }

  @Test
  public void T_dumpShort_1(){
    List<Short> list = new ArrayList<Short>();
    for( int i = 1000 ; i < 2000 ; i++ ){
      if( ( i % 3 ) == 0 ){
        list.add( Integer.valueOf( i ).shortValue() );
      }
    }
    byte[] binary = BinaryDump.dumpShort( list );
    List<Short> decodeList = BinaryDump.binaryToShortList( binary , 0 , binary.length );
    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_dumpShort_2(){
    List<Short> list = new ArrayList<Short>();
    byte[] binary = BinaryDump.dumpShort( list );
    List<Short> decodeList = BinaryDump.binaryToShortList( binary , 0 , binary.length );
    assertEquals( list.size() , decodeList.size() );
    assertEquals( binary.length , 0 );
  }

  @Test
  public void T_dumpShort_3(){
    List<Short> list = null;
    byte[] binary = BinaryDump.dumpShort( list );
    List<Short> decodeList = BinaryDump.binaryToShortList( binary , 0 , binary.length );
    assertEquals( decodeList.size() , 0 );
    assertEquals( binary.length , 0 );
  }

  @Test
  public void T_dumpInteger_1(){
    List<Integer> list = new ArrayList<Integer>();
    for( int i = Integer.MAX_VALUE - 1000 ; i < Integer.MAX_VALUE ; i++ ){
      if( ( i % 3 ) == 0 ){
        list.add( Integer.valueOf( i ) );
      }
    }
    byte[] binary = BinaryDump.dumpInteger( list );
    List<Integer> decodeList = BinaryDump.binaryToIntegerList( binary , 0 , binary.length );
    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_dumpInteger_2(){
    List<Integer> list = new ArrayList<Integer>();
    byte[] binary = BinaryDump.dumpInteger( list );
    List<Integer> decodeList = BinaryDump.binaryToIntegerList( binary , 0 , binary.length );
    assertEquals( list.size() , decodeList.size() );
    assertEquals( binary.length , 0 );
  }

  @Test
  public void T_dumpInteger_3(){
    List<Integer> list = null;
    byte[] binary = BinaryDump.dumpInteger( list );
    List<Integer> decodeList = BinaryDump.binaryToIntegerList( binary , 0 , binary.length );
    assertEquals( decodeList.size() , 0 );
    assertEquals( binary.length , 0 );
  }


  @Test
  public void T_dumpLong_1(){
    List<Long> list = new ArrayList<Long>();
    for( long i = Long.MAX_VALUE - 1000 ; i < Long.MAX_VALUE ; i++ ){
      if( ( i % 3 ) == 0 ){
        list.add( Long.valueOf( i ) );
      }
    }
    byte[] binary = BinaryDump.dumpLong( list );
    List<Long> decodeList = BinaryDump.binaryToLongList( binary , 0 , binary.length );
    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_dumpLong_2(){
    List<Long> list = new ArrayList<Long>();
    byte[] binary = BinaryDump.dumpLong( list );
    List<Long> decodeList = BinaryDump.binaryToLongList( binary , 0 , binary.length );
    assertEquals( list.size() , decodeList.size() );
    assertEquals( binary.length , 0 );
  }

  @Test
  public void T_dumpLong_3(){
    List<Long> list = null;
    byte[] binary = BinaryDump.dumpLong( list );
    List<Long> decodeList = BinaryDump.binaryToLongList( binary , 0 , binary.length );
    assertEquals( decodeList.size() , 0 );
    assertEquals( binary.length , 0 );
  }


}
