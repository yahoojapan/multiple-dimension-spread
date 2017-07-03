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
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.IntBuffer;

import java.util.List;
import java.util.ArrayList;

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveType;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;
import jp.co.yahoo.dataplatform.schema.objects.DoubleObj;
import jp.co.yahoo.dataplatform.schema.objects.IntegerObj;
import jp.co.yahoo.dataplatform.schema.objects.LongObj;
import jp.co.yahoo.dataplatform.schema.objects.ShortObj;
import jp.co.yahoo.dataplatform.schema.objects.FloatObj;
import jp.co.yahoo.dataplatform.schema.objects.ByteObj;

import jp.co.yahoo.dataplatform.mds.binary.maker.IPrimitiveObjectConnector;

import static jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength.CHAR_LENGTH;
import static jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength.SHORT_LENGTH;
import static jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength.INT_LENGTH;
import static jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength.LONG_LENGTH;
import static jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength.FLOAT_LENGTH;
import static jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength.DOUBLE_LENGTH;

public final class BinaryDump{

  private BinaryDump(){}

  public static byte[] dumpByte( final List<Byte> dumpTarget ){
    if( dumpTarget == null ){
      return new byte[0];
    }
    byte[] result = new byte[dumpTarget.size()];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    for( Byte obj : dumpTarget ){
      wrapBuffer.put( obj.byteValue() );
    }

    return result;
  }

  public static List<Byte> binaryToByteList( final byte[] binary , final int start , final int length ){
    List<Byte> result = new ArrayList<Byte>();
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    for( int i = 0 ; i < length ; i++ ){
      result.add( Byte.valueOf( wrapBuffer.get() ) );
    }

    return result;
  }

  public static PrimitiveObject[] binaryToByteArray( final byte[] binary , final int start , final int length , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    PrimitiveObject[] result = new PrimitiveObject[length];
    for( int i = 0 ; i < length ; i++ ){
      result[i] = primitiveObjectConnector.convert( PrimitiveType.BYTE , new ByteObj( wrapBuffer.get() ) );
    }

    return result;
  }

  public static byte[] dumpBytes( final List<byte[]> dumpTarget , final int totalLength ){
    byte[] result = new byte[ INT_LENGTH * dumpTarget.size() + totalLength ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    int offset = 0;
    for( byte[] obj : dumpTarget ){
      int length = obj.length;
      wrapBuffer.putInt( offset , length );
      offset += INT_LENGTH;
      System.arraycopy( obj , 0 , result , offset , length );

      offset+= length;
    }

    return result;
  }

  public static void appendBytesToByteBuffer( final List<byte[]> dumpTarget , final int totalLength , final ByteBuffer rawByteBuffer ){
    for( byte[] obj : dumpTarget ){
      rawByteBuffer.putInt( obj.length );
      rawByteBuffer.put( obj );
    }
  }

  public static List<byte[]> binaryToBytesList( final byte[] binary , final int start , final int length ){
    int offset = start;
    List<byte[]> result = new ArrayList<byte[]>();
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    while( offset < ( start + length ) ){
      int dataLength = wrapBuffer.getInt( offset );
      offset += INT_LENGTH;
      byte[] data = new byte[dataLength];
      System.arraycopy( binary , offset , data , 0 , dataLength );
      result.add( data );
      offset += dataLength;
    }
    return result;
  }

  public static List<PrimitiveObject> binaryToUTF8BytesLinkObjList( final byte[] binary , final int start , final int length , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
    int offset = start;
    List<PrimitiveObject> result = new ArrayList<PrimitiveObject>();
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    while( offset < ( start + length ) ){
      int dataLength = wrapBuffer.getInt( offset );
      offset += INT_LENGTH;
      result.add( primitiveObjectConnector.convert( PrimitiveType.STRING , new UTF8BytesLinkObj( binary , offset , dataLength ) ) );
      offset += dataLength;
    }
    return result;
  }

  public static void binaryToUTF8BytesLinkObjList( final byte[] binary , final int start , final int length , final IPrimitiveObjectConnector primitiveObjectConnector , final List<PrimitiveObject> result ) throws IOException{
    int offset = start;
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    int endOffset = start + length;
    while( offset < endOffset ){
      int dataLength = wrapBuffer.getInt( offset );
      offset += INT_LENGTH;
      result.add( primitiveObjectConnector.convert( PrimitiveType.STRING , new UTF8BytesLinkObj( binary , offset , dataLength ) ) );
      offset += dataLength;
    }
  }

  public static byte[] dumpString( final List<String> dumpTarget , final int totalLength ){
    byte[] result = new byte[ INT_LENGTH + INT_LENGTH * dumpTarget.size() + totalLength ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    wrapBuffer.putInt( 0 , dumpTarget.size() );
    int lengthOffset = INT_LENGTH;
    CharBuffer viewCharBuffer = wrapBuffer.asCharBuffer();
    int charOffset = ( INT_LENGTH + INT_LENGTH * dumpTarget.size() ) / CHAR_LENGTH;

    for( String obj : dumpTarget ){
      char[] objChars = obj.toCharArray();
      int charLength = objChars.length;
      wrapBuffer.putInt( lengthOffset , charLength );
      lengthOffset += INT_LENGTH;
      viewCharBuffer.position( charOffset );
      viewCharBuffer.put( objChars );
      charOffset += charLength;
    }

    return result;
  }

  public static void appendStringToByteBuffer( final List<String> dumpTarget , final int totalLength , final ByteBuffer rawByteBuffer ){
    rawByteBuffer.putInt( dumpTarget.size() );

    CharBuffer viewCharBuffer = rawByteBuffer.asCharBuffer();
    viewCharBuffer.position( ( INT_LENGTH * dumpTarget.size() ) / CHAR_LENGTH );

    for( String obj : dumpTarget ){
      char[] objChars = obj.toCharArray();
      int charLength = objChars.length;
      rawByteBuffer.putInt( charLength );
      viewCharBuffer.put( objChars );
    }

    rawByteBuffer.position( rawByteBuffer.position() + totalLength );
  }

  public static List<String> binaryToStringList( final byte[] binary , final int start , final int length ){
    List<String> result = new ArrayList<String>();
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    int rows = wrapBuffer.getInt( start );
    int lengthOffset = start + INT_LENGTH;

    CharBuffer viewCharBuffer = wrapBuffer.asCharBuffer();
    int charOffset = ( INT_LENGTH + INT_LENGTH * rows ) / CHAR_LENGTH;
    for( int i = 0 ; i < rows ; i++ ){
      int charLength = wrapBuffer.getInt( lengthOffset );
      lengthOffset+=INT_LENGTH;
      viewCharBuffer.position( charOffset );
      char[] chars = new char[charLength];
      viewCharBuffer.get( chars );
      result.add( String.valueOf( chars ) );
      charOffset += charLength;
    }
    return result;
  }

  public static byte[] dumpDouble( final List<Double> dumpTarget ){
    if( dumpTarget == null ){
      return new byte[0];
    }
    byte[] result = new byte[DOUBLE_LENGTH*dumpTarget.size()];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    for( Double obj : dumpTarget ){
      wrapBuffer.putDouble( obj.doubleValue() );
    }

    return result;
  }

  public static List<Double> binaryToDoubleList( final byte[] binary , final int start , final int length ){
    List<Double> result = new ArrayList<Double>();
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    int loopCount = length / DOUBLE_LENGTH;
    for( int i = 0 ; i < loopCount ; i++ ){
      result.add( wrapBuffer.getDouble() );
    }

    return result;
  }

  public static PrimitiveObject[] binaryToDoubleArray( final byte[] binary , final int start , final int length , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    int loopCount = length / DOUBLE_LENGTH;
    PrimitiveObject[] result = new PrimitiveObject[loopCount];
    for( int i = 0 ; i < loopCount ; i++ ){
      result[i] = primitiveObjectConnector.convert( PrimitiveType.DOUBLE , new DoubleObj( wrapBuffer.getDouble() ) );
    }

    return result;
  }

  public static void binaryToDoubleList( final byte[] binary , final int start , final int length , final IPrimitiveObjectConnector primitiveObjectConnector , final List<PrimitiveObject> result ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    int loopCount = length / DOUBLE_LENGTH;
    for( int i = 0 ; i < loopCount ; i++ ){
      result.add( primitiveObjectConnector.convert( PrimitiveType.DOUBLE , new DoubleObj( wrapBuffer.getDouble() ) ) );
    }
  }

  public static byte[] dumpFloat( final List<Float> dumpTarget ){
    if( dumpTarget == null ){
      return new byte[0];
    }
    byte[] result = new byte[ FLOAT_LENGTH * dumpTarget.size() ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    for( Float obj : dumpTarget ){
      wrapBuffer.putFloat( obj.floatValue() );
    }

    return result;
  }

  public static List<Float> binaryToFloatList( final byte[] binary , final int start , final int length ){
    int offset = start;
    List<Float> result = new ArrayList<Float>();
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    while( offset < ( start + length ) ){
      result.add( wrapBuffer.getFloat( offset ) );
      offset+=FLOAT_LENGTH;
    }

    return result;
  }

  public static PrimitiveObject[] binaryToFloatArray( final byte[] binary , final int start , final int length , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    int loopCount = length / FLOAT_LENGTH;
    PrimitiveObject[] result = new PrimitiveObject[loopCount];
    for( int i = 0 ; i < loopCount ; i++ ){
      result[i] = primitiveObjectConnector.convert( PrimitiveType.FLOAT , new FloatObj( wrapBuffer.getFloat() ) );
    }

    return result;
  }

  public static byte[] dumpShort( final List<Short> dumpTarget ){
    if( dumpTarget == null ){
      return new byte[0];
    }
    byte[] result = new byte[ SHORT_LENGTH * dumpTarget.size() ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    for( Short obj : dumpTarget ){
      wrapBuffer.putShort( obj.shortValue() );
    }

    return result;
  }

  public static List<Short> binaryToShortList( final byte[] binary , final int start , final int length ){
    int offset = start;
    List<Short> result = new ArrayList<Short>();
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    while( offset < ( start + length ) ){
      result.add( Short.valueOf( wrapBuffer.getShort( offset ) ) );
      offset+=SHORT_LENGTH;
    }

    return result;
  }

  public static PrimitiveObject[] binaryToShortArray( final byte[] binary , final int start , final int length , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    int loopCount = length / SHORT_LENGTH;
    PrimitiveObject[] result = new PrimitiveObject[loopCount];
    for( int i = 0 ; i < loopCount ; i++ ){
      result[i] = primitiveObjectConnector.convert( PrimitiveType.SHORT , new ShortObj( wrapBuffer.getShort() ) );
    }

    return result;
  }

  public static byte[] dumpInteger( final List<Integer> dumpTarget ){
    if( dumpTarget == null ){
      return new byte[0];
    }
    byte[] result = new byte[ INT_LENGTH * dumpTarget.size() ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    for( Integer obj : dumpTarget ){
      wrapBuffer.putInt( obj.intValue() );
    }

    return result;
  }

  public static void appendIntegerToByteBuffer( final List<Integer> dumpTarget , final ByteBuffer rawByteBuffer ){
    for( Integer obj : dumpTarget ){
      rawByteBuffer.putInt( obj.intValue() );
    }
  }

  public static List<Integer> binaryToIntegerList( final byte[] binary , final int start , final int length ){
    int rows = length / INT_LENGTH;
    List<Integer> result = new ArrayList<Integer>( rows );
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    for( int i = 0 ; i < rows ; i++ ){
      result.add( Integer.valueOf( wrapBuffer.getInt() ) );
    }

    return result;
  }

  public static void binaryToIntegerList( final byte[] binary , final int start , final int length , final IPrimitiveObjectConnector primitiveObjectConnector , final List<PrimitiveObject> result ) throws IOException{
    int rows = length / INT_LENGTH;
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    for( int i = 0 ; i < rows ; i++ ){
      result.add( primitiveObjectConnector.convert( PrimitiveType.INTEGER , new IntegerObj( wrapBuffer.getInt() ) ) );
    }
  }

  public static PrimitiveObject[] binaryToIntegerArray( final byte[] binary , final int start , final int length , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
    int rows = length / INT_LENGTH;
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    PrimitiveObject[] result = new PrimitiveObject[rows];
    for( int i = 0 ; i < rows ; i++ ){
      result[i] = primitiveObjectConnector.convert( PrimitiveType.INTEGER , new IntegerObj( wrapBuffer.getInt() ) );
    }

    return result;
  }

  public static void binaryToIntegerList( final byte[] binary , final int start , final int length , final List<Integer> target ){
    target.clear();
    int rows = length / INT_LENGTH;
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    for( int i = 0 ; i < rows ; i++ ){
      target.add( Integer.valueOf( wrapBuffer.getInt() ) );
    }
  }

  public static IntBuffer binaryToIntBuffer( final byte[] binary , final int start , final int length ){
    return ByteBuffer.wrap( binary , start , length ).asIntBuffer();
  }

  public static byte[] dumpLong( final List<Long> dumpTarget ){
    if( dumpTarget == null ){
      return new byte[0];
    }
    byte[] result = new byte[ LONG_LENGTH * dumpTarget.size() ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    for( Long obj : dumpTarget ){
      wrapBuffer.putLong( obj.longValue() );
    }

    return result;
  }

  public static List<Long> binaryToLongList( final byte[] binary , final int start , final int length ){
    List<Long> result = new ArrayList<Long>();
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    int loopCount = length / LONG_LENGTH;
    for( int i = 0 ; i < loopCount ; i++ ){
      result.add( wrapBuffer.getLong() );
    }

    return result;
  }

  public static void binaryToLongList( final byte[] binary , final int start , final int length , final IPrimitiveObjectConnector primitiveObjectConnector , final List<PrimitiveObject> result ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    int loopCount = length / LONG_LENGTH;
    for( int i = 0 ; i < loopCount ; i++ ){
      result.add( primitiveObjectConnector.convert( PrimitiveType.LONG , new LongObj( wrapBuffer.getLong() ) ) );
    }
  }

  public static PrimitiveObject[] binaryToLongArray( final byte[] binary , final int start , final int length , final IPrimitiveObjectConnector primitiveObjectConnector ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    int loopCount = length / LONG_LENGTH;
    PrimitiveObject[] result = new PrimitiveObject[loopCount];
    for( int i = 0 ; i < loopCount ; i++ ){
      result[i] = primitiveObjectConnector.convert( PrimitiveType.LONG , new LongObj( wrapBuffer.getLong() ) );
    }

    return result;
  }

}
