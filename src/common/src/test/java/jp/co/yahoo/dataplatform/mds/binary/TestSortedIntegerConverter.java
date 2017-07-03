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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;
import org.testng.annotations.Test;


import java.util.List;
import java.util.ArrayList;

public class TestSortedIntegerConverter{

  @Test
  public void T_getBinary_1() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    for( int i = 0 ; i < 500 ; i++ ){
      list.add( Integer.valueOf( i ) );
    }
    byte[] binary = SortedIntegerConverter.getBinary( list );
    //assertEquals( SortedIntegerConverter.getBitType( binary[0] ) , SortedIntegerConverter.CompressBit.COMPRESS_1 );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );
    
    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_getBinary_2() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    for( int i = 0 ; i < 500 ; i += ( ( 1 << 2 ) -1 ) ){
      list.add( Integer.valueOf( i ) );
    }
    byte[] binary = SortedIntegerConverter.getBinary( list );
    //assertEquals( SortedIntegerConverter.getBitType( binary[0] ) , SortedIntegerConverter.CompressBit.COMPRESS_2 );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_getBinary_3() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    for( int i = 0 ; i < 500 ; i += ( ( 1 << 4 ) -1 ) ){
      list.add( Integer.valueOf( i ) );
    }
    byte[] binary = SortedIntegerConverter.getBinary( list );
    //assertEquals( SortedIntegerConverter.getBitType( binary[0] ) , SortedIntegerConverter.CompressBit.COMPRESS_4 );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_getBinary_4() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    for( int i = 0 ; i < 5000 ; i += ( ( 1 << 8 ) -1 ) ){
      list.add( Integer.valueOf( i ) );
    }
    byte[] binary = SortedIntegerConverter.getBinary( list );
    //assertEquals( SortedIntegerConverter.getBitType( binary[0] ) , SortedIntegerConverter.CompressBit.COMPRESS_8 );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_getBinary_5() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    for( int i = 0 ; i < 1000000 ; i += ( ( 1 << 16 ) - 1 ) ){
      list.add( Integer.valueOf( i ) );
    }
    byte[] binary = SortedIntegerConverter.getBinary( list );
    //assertEquals( SortedIntegerConverter.getBitType( binary[0] ) , SortedIntegerConverter.CompressBit.COMPRESS_16 );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_getBinary_6() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    for( int i = 0 ; i < 1 ; i++ ){
      list.add( Integer.valueOf( i ) );
    }
    byte[] binary = SortedIntegerConverter.getBinary( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_compressInteger16_1() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    byte[] binary = SortedIntegerConverter.compressInteger16( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );
    assertTrue( decodeList.isEmpty() );
  }

  @Test
  public void T_compressInteger16_2() throws IOException{
    byte[] binary = SortedIntegerConverter.compressInteger16( null );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );
    assertTrue( decodeList.isEmpty() );
  }

  @Test
  public void T_compressInteger16_3() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    list.add( 0 );
    list.add( ( 1 << 16 ) );
    byte[] binary = SortedIntegerConverter.compressInteger16( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_compressInteger8_1() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    byte[] binary = SortedIntegerConverter.compressInteger8( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );
    assertTrue( decodeList.isEmpty() );
  }

  @Test
  public void T_compressInteger8_2() throws IOException{
    byte[] binary = SortedIntegerConverter.compressInteger8( null );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );
    assertTrue( decodeList.isEmpty() );
  }

  @Test
  public void T_compressInteger8_3() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    list.add( 0 );
    list.add( ( 1 << 8 ) );
    byte[] binary = SortedIntegerConverter.compressInteger8( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_compressInteger4_1() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    list.add( 100 );
    list.add( 101 );
    list.add( 102 );
    byte[] binary = SortedIntegerConverter.compressInteger4( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_compressInteger4_2() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    list.add( 100 );
    list.add( 101 );
    list.add( 102 );
    list.add( 103 );
    byte[] binary = SortedIntegerConverter.compressInteger4( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_compressInteger4_3() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    byte[] binary = SortedIntegerConverter.compressInteger4( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );
    assertTrue( decodeList.isEmpty() );
  }

  @Test
  public void T_compressInteger4_4() throws IOException{
    byte[] binary = SortedIntegerConverter.compressInteger4( null );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );
    assertTrue( decodeList.isEmpty() );
  }

  @Test
  public void T_compressInteger4_5() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    list.add( 0 );
    list.add( ( 1 << 4 ) );
    byte[] binary = SortedIntegerConverter.compressInteger4( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_compressInteger2_1() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    list.add( 100 );
    list.add( 101 );
    list.add( 102 );
    list.add( 103 );
    list.add( 200 );
    byte[] binary = SortedIntegerConverter.compressInteger2( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_compressInteger2_2() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    list.add( 100 );
    list.add( 101 );
    list.add( 102 );
    list.add( 103 );
    list.add( 200 );
    list.add( 201 );
    byte[] binary = SortedIntegerConverter.compressInteger2( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_compressInteger2_3() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    list.add( 100 );
    list.add( 101 );
    list.add( 102 );
    list.add( 103 );
    list.add( 200 );
    list.add( 201 );
    list.add( 202 );
    byte[] binary = SortedIntegerConverter.compressInteger2( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_compressInteger2_4() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    list.add( 100 );
    list.add( 101 );
    list.add( 102 );
    list.add( 103 );
    list.add( 200 );
    list.add( 201 );
    list.add( 202 );
    list.add( 203 );
    byte[] binary = SortedIntegerConverter.compressInteger2( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_compressInteger2_5() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    byte[] binary = SortedIntegerConverter.compressInteger2( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );
    assertTrue( decodeList.isEmpty() );
  }

  @Test
  public void T_compressInteger2_6() throws IOException{
    byte[] binary = SortedIntegerConverter.compressInteger2( null );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );
    assertTrue( decodeList.isEmpty() );
  }

  @Test
  public void T_compressInteger2_7() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    list.add( 0 );
    list.add( ( 1 << 2 ) );
    byte[] binary = SortedIntegerConverter.compressInteger2( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_compressInteger1_1() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    list.add( 100 );
    list.add( 101 );
    list.add( 102 );
    list.add( 103 );
    list.add( 200 );
    byte[] binary = SortedIntegerConverter.compressInteger1( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_compressInteger1_2() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    list.add( 100 );
    list.add( 101 );
    list.add( 102 );
    list.add( 103 );
    list.add( 200 );
    list.add( 201 );
    byte[] binary = SortedIntegerConverter.compressInteger1( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_compressInteger1_3() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    list.add( 100 );
    list.add( 101 );
    list.add( 102 );
    list.add( 103 );
    list.add( 200 );
    list.add( 201 );
    list.add( 202 );
    byte[] binary = SortedIntegerConverter.compressInteger1( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_compressInteger1_4() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    list.add( 100 );
    list.add( 101 );
    list.add( 102 );
    list.add( 103 );
    list.add( 200 );
    list.add( 201 );
    list.add( 202 );
    list.add( 203 );
    byte[] binary = SortedIntegerConverter.compressInteger1( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_compressInteger1_5() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    list.add( 100 );
    list.add( 101 );
    list.add( 102 );
    list.add( 103 );
    list.add( 200 );
    list.add( 201 );
    list.add( 202 );
    list.add( 203 );
    list.add( 204 );
    byte[] binary = SortedIntegerConverter.compressInteger1( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_compressInteger1_6() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    list.add( 100 );
    list.add( 101 );
    list.add( 102 );
    list.add( 103 );
    list.add( 200 );
    list.add( 201 );
    list.add( 202 );
    list.add( 203 );
    list.add( 204 );
    list.add( 205 );
    byte[] binary = SortedIntegerConverter.compressInteger1( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_compressInteger1_7() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    list.add( 100 );
    list.add( 101 );
    list.add( 102 );
    list.add( 103 );
    list.add( 200 );
    list.add( 201 );
    list.add( 202 );
    list.add( 203 );
    list.add( 204 );
    list.add( 205 );
    list.add( 206 );
    byte[] binary = SortedIntegerConverter.compressInteger1( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_compressInteger1_8() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    list.add( 100 );
    list.add( 101 );
    list.add( 102 );
    list.add( 103 );
    list.add( 200 );
    list.add( 201 );
    list.add( 202 );
    list.add( 203 );
    list.add( 204 );
    list.add( 205 );
    list.add( 206 );
    list.add( 207 );
    byte[] binary = SortedIntegerConverter.compressInteger1( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }

  @Test
  public void T_compressInteger1_9() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    byte[] binary = SortedIntegerConverter.compressInteger1( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );
    assertTrue( decodeList.isEmpty() );
  }

  @Test
  public void T_compressInteger1_10() throws IOException{
    byte[] binary = SortedIntegerConverter.compressInteger1( null );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );
    assertTrue( decodeList.isEmpty() );
  }

  @Test
  public void T_compressInteger1_11() throws IOException{
    List<Integer> list = new ArrayList<Integer>();
    list.add( 0 );
    list.add( ( 1 << 1 ) );
    byte[] binary = SortedIntegerConverter.compressInteger1( list );
    List<Integer> decodeList = SortedIntegerConverter.getIntegerList( binary , 0 , binary.length );

    for( int i = 0 ; i < list.size() ; i++ ){
      assertEquals( list.get(i) , decodeList.get(i) );
    }
    assertEquals( list.size() , decodeList.size() );
  }


}
