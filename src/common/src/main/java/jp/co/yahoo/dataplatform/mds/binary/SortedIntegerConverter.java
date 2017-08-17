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

import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.List;
import java.util.ArrayList;

public final class SortedIntegerConverter{

  public static final int BYTE_2 = 1 << 16;
  public static final int BYTE_1 = 1 << 8;
  public static final int BIT_4 = 1 << 4;
  public static final int BIT_2 = 1 << 2;
  public static final int BIT_1 = 1 << 1;

  private SortedIntegerConverter(){}

  public enum CompressBit{
    DUMP_INTEGER,
    COMPRESS_16,
    COMPRESS_8,
    COMPRESS_4,
    COMPRESS_2,
    COMPRESS_1,
  }

  private static final CompressBit[] COMPARE_TARGET = new CompressBit[]{ CompressBit.COMPRESS_16 , CompressBit.COMPRESS_8 , CompressBit.COMPRESS_4 , CompressBit.COMPRESS_2 , CompressBit.COMPRESS_1 };

  public static byte[] getBinary( final List<Integer> sortedIndexList ) throws IOException{
    int rows = sortedIndexList.size();
    if( rows < 2 ){
      return dumpInteger( sortedIndexList );
    }
    int minIndex = sortedIndexList.get( 0 ).intValue();
    int maxIndex = sortedIndexList.get( rows - 1 ).intValue();

    int maxDiff = maxIndex - minIndex;
    switch( chooseCompressType( maxDiff , rows ) ){
      case COMPRESS_16:
        return compressInteger16( sortedIndexList );
      case COMPRESS_8:
        return compressInteger8( sortedIndexList );
      case COMPRESS_4:
        return compressInteger4( sortedIndexList );
      case COMPRESS_2:
        return compressInteger2( sortedIndexList );
      case COMPRESS_1:
        return compressInteger1( sortedIndexList );
      case DUMP_INTEGER:
      default:
        return dumpInteger( sortedIndexList );
    }
  }

  public static CompressBit chooseCompressType( final int maxDiff , final int rows ) {
    int intDumpSize = 4 * rows;
    int headerSize = 1 + 4 + 4;
    double skipIndexRate = ( intToDouble( maxDiff ) - intToDouble( rows ) ) / intToDouble( maxDiff );

    CompressBit minType = CompressBit.DUMP_INTEGER;
    int minTotal = Integer.MAX_VALUE;
    for( CompressBit needBit : COMPARE_TARGET ){
      int bitNumber = getBitNumber( needBit );
      double rowsPerByte = intToDouble( bitNumber ) / Double.valueOf(8);
      int bucketNumber = Double.valueOf( Math.ceil( intToDouble( maxDiff ) /  intToDouble( getBitToInt( needBit ) ) ) ).intValue();
      int body;
      if( bucketNumber == 1 ){
        body = Double.valueOf( Math.ceil( rowsPerByte * intToDouble( rows ) ) ).intValue();
      }
      else{
        body = Double.valueOf( Math.ceil( intToDouble( bucketNumber ) * skipIndexRate ) ).intValue();
      }
      int total = ( headerSize + Double.valueOf( rowsPerByte * ( intToDouble( rows ) / intToDouble( body ) ) ).intValue() ) * body;
      if( total <= minTotal ){
        minTotal = total;
        minType = needBit;
      }
    }
    if( intDumpSize <= minTotal ){
      return CompressBit.DUMP_INTEGER;
    }
    else{
      return minType;
    }
  }

  public static List<Integer> getFromDumpInteger( final byte[] buffer , final int start , final int length ){
    List<Integer> result = new ArrayList<Integer>();
    ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start + 1 , length - 1 );
    for( int i = 0 ; i < ( ( length - 1 ) / 4 ) ; i++ ){
      result.add( Integer.valueOf( wrapBuffer.getInt() ) );
    }
    return result;
  }

  public static byte[] dumpInteger( final List<Integer> dumpTarget ){
    byte[] result = new byte[ 1 + 4 * dumpTarget.size() ];
    int offset = 0;
    result[offset] = (byte)0;
    offset++;

    for( Integer indexObj : dumpTarget ){
      int index = indexObj.intValue();
      result[offset] = (byte)( index >>> 24 );
      result[offset+1] = (byte)( index >>> 16 );
      result[offset+2] = (byte)( index >>> 8 );
      result[offset+3] = (byte)( index );
      offset+=4;
    }

    return result;
  }

  public static Integer readInt( final byte[] buffer , final int start , final int length ){
    ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer , start , length );
    return wrapBuffer.getInt( start );
  }

  public static void writeInt( final OutputStream out , final int writeInt ) throws IOException{
    ByteBuffer buffer = ByteBuffer.allocate( 4 );
    buffer.putInt( writeInt );
    out.write( buffer.array() , 0 , 4 );
  }

  private static List<Integer> decompressInteger1( final byte[] buffer , final int start , final int length ){
    List<Integer> result = new ArrayList<Integer>( length * 8 );
    int offset = start+1;
    int shiftBit = getBitNumber( CompressBit.COMPRESS_1 );
    int shiftBit2 = shiftBit * 2;
    int shiftBit3 = shiftBit * 3;
    int shiftBit4 = shiftBit * 4;
    int shiftBit5 = shiftBit * 5;
    int shiftBit6 = shiftBit * 6;
    int shiftBit7 = shiftBit * 7;
    while( offset < ( start + length ) ){
      int diffIndex = readInt( buffer , offset , 4 );
      offset += 4;
      int size = readInt( buffer , offset , 4 );
      offset += 4;
      for( int i = 0 ; i < size ; i+=8 ){
        int compressInt = buffer[offset] & 255;
        int diff1 = compressInt & 1;
        int diff2 = ( compressInt >>> shiftBit ) & 1;
        int diff3 = ( compressInt >>> ( shiftBit2 ) ) & 1;
        int diff4 = ( compressInt >>> ( shiftBit3 ) ) & 1;
        int diff5 = ( compressInt >>> ( shiftBit4 ) ) & 1;
        int diff6 = ( compressInt >>> ( shiftBit5 ) ) & 1;
        int diff7 = ( compressInt >>> ( shiftBit6 ) ) & 1;
        int diff8 = ( compressInt >>> ( shiftBit7 ) ) & 1;
        diffIndex += diff1;
        result.add( diffIndex );
        if( i + 1 < size ){
          diffIndex += diff2;
          result.add( diffIndex );
        }
        if( i + 2 < size ){
          diffIndex += diff3;
          result.add( diffIndex );
        }
        if( i + 3 < size ){
          diffIndex += diff4;
          result.add( diffIndex );
        }
        if( i + 4 < size ){
          diffIndex += diff5;
          result.add( diffIndex );
        }
        if( i + 5 < size ){
          diffIndex += diff6;
          result.add( diffIndex );
        }
        if( i + 6 < size ){
          diffIndex += diff7;
          result.add( diffIndex );
        }
        if( i + 7 < size ){
          diffIndex += diff8;
          result.add( diffIndex );
        }
        offset++;
      }
    }
    return result;
  }

  private static void appendInteger1( final OutputStream out , final int startIndex , final List<Integer> diffList ) throws IOException{
    int length = diffList.size();
    writeInt( out , startIndex );
    writeInt( out , length );

    int listSize = diffList.size();
    int shiftBit = getBitNumber( CompressBit.COMPRESS_1 );
    int shift2 = shiftBit;
    int shift3 = shiftBit * 2;
    int shift4 = shiftBit * 3;
    int shift5 = shiftBit * 4;
    int shift6 = shiftBit * 5;
    int shift7 = shiftBit * 6;
    int shift8 = shiftBit * 7;
    for( int i = 0 ; i < listSize ; i+=8 ){
      if( listSize == i + 1 ){
        int diff1 = diffList.get(i).intValue();
        out.write( diff1 );
      }
      else if( listSize == i + 2 ){
        int diff1 = diffList.get(i).intValue();
        int diff2 = diffList.get(i+1).intValue() << shift2;
        out.write( diff1 + diff2 );
      }
      else if( listSize == i + 3 ){
        int diff1 = diffList.get(i).intValue();
        int diff2 = diffList.get(i+1).intValue() << shift2;
        int diff3 = diffList.get(i+2).intValue() << shift3;
        out.write( diff1 + diff2 + diff3 );
      }
      else if( listSize == i + 4 ){
        int diff1 = diffList.get(i).intValue();
        int diff2 = diffList.get(i+1).intValue() << shift2;
        int diff3 = diffList.get(i+2).intValue() << shift3;
        int diff4 = diffList.get(i+3).intValue() << shift4;
        out.write( diff1 + diff2 + diff3 + diff4 );
      }
      else if( listSize == i + 5 ){
        int diff1 = diffList.get(i).intValue();
        int diff2 = diffList.get(i+1).intValue() << shift2;
        int diff3 = diffList.get(i+2).intValue() << shift3;
        int diff4 = diffList.get(i+3).intValue() << shift4;
        int diff5 = diffList.get(i+4).intValue() << shift5;
        out.write( diff1 + diff2 + diff3 + diff4 + diff5 );
      }
      else if( listSize == i + 6 ){
        int diff1 = diffList.get(i).intValue();
        int diff2 = diffList.get(i+1).intValue() << shift2;
        int diff3 = diffList.get(i+2).intValue() << shift3;
        int diff4 = diffList.get(i+3).intValue() << shift4;
        int diff5 = diffList.get(i+4).intValue() << shift5;
        int diff6 = diffList.get(i+5).intValue() << shift6;
        out.write( diff1 + diff2 + diff3 + diff4 + diff5 + diff6 );
      }
      else if( listSize == i + 7 ){
        int diff1 = diffList.get(i).intValue();
        int diff2 = diffList.get(i+1).intValue() << shift2;
        int diff3 = diffList.get(i+2).intValue() << shift3;
        int diff4 = diffList.get(i+3).intValue() << shift4;
        int diff5 = diffList.get(i+4).intValue() << shift5;
        int diff6 = diffList.get(i+5).intValue() << shift6;
        int diff7 = diffList.get(i+6).intValue() << shift7;
        out.write( diff1 + diff2 + diff3 + diff4 + diff5 + diff6 + diff7 );
      }
      else{
        int diff1 = diffList.get(i).intValue();
        int diff2 = diffList.get(i+1).intValue() << shift2;
        int diff3 = diffList.get(i+2).intValue() << shift3;
        int diff4 = diffList.get(i+3).intValue() << shift4;
        int diff5 = diffList.get(i+4).intValue() << shift5;
        int diff6 = diffList.get(i+5).intValue() << shift6;
        int diff7 = diffList.get(i+6).intValue() << shift7;
        int diff8 = diffList.get(i+7).intValue() << shift8;
        out.write( diff1 + diff2 + diff3 + diff4 + diff5 + diff6 + diff7 + diff8 );
      }
    }

  }

  public static byte[] compressInteger1( final List<Integer> dumpTarget )throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write( getBitNumber( CompressBit.COMPRESS_1 ) );
    if( dumpTarget == null || dumpTarget.isEmpty() ){
      return out.toByteArray();
    }

    int startIndex = dumpTarget.get(0).intValue();
    int before = dumpTarget.get(0).intValue();
    List<Integer> diffList = new ArrayList<Integer>();
    for( int i = 0 ; i < dumpTarget.size() ; i++ ){
      int target = dumpTarget.get(i).intValue();
      int diff = target - before;
      if( BIT_1 <= diff ){
        appendInteger1( out , startIndex , diffList );

        diffList.clear();
        startIndex = target;
        before = target;
        diff = 0;
      }
      else{
        before = target;
      }
      diffList.add( Integer.valueOf( diff ) );
    }
    appendInteger1( out , startIndex , diffList );

    return out.toByteArray();
  }

  private static List<Integer> decompressInteger2( final byte[] buffer , final int start , final int length ){
    List<Integer> result = new ArrayList<Integer>();
    int offset = start+1;
    int shiftBit = getBitNumber( CompressBit.COMPRESS_2 );
    while( offset < ( start + length ) ){
      int diffIndex = readInt( buffer , offset , 4 );
      offset += 4;
      int size = readInt( buffer , offset , 4 );
      offset += 4;
      for( int i = 0 ; i < size ; i+=4 ){
        int compressInt = buffer[offset] & 255;
        int diff1 = compressInt & 3;
        int diff2 = ( compressInt >>> shiftBit ) & 3;
        int diff3 = ( compressInt >>> ( shiftBit * 2 ) ) & 3;
        int diff4 = ( compressInt >>> ( shiftBit * 3 ) ) & 3;
        diffIndex += diff1;
        result.add( diffIndex );
        if( i + 1 < size ){
          diffIndex += diff2;
          result.add( diffIndex );
        }
        if( i + 2 < size ){
          diffIndex += diff3;
          result.add( diffIndex );
        }
        if( i + 3 < size ){
          diffIndex += diff4;
          result.add( diffIndex );
        }
        offset++;
      }
    }
    return result;
  }

  private static void appendInteger2( final OutputStream out , final int startIndex , final List<Integer> diffList ) throws IOException{
    int length = diffList.size();
    writeInt( out , startIndex );
    writeInt( out , length );

    int listSize = diffList.size();
    int shiftBit = getBitNumber( CompressBit.COMPRESS_2 );
    int shift2 = shiftBit;
    int shift3 = shiftBit * 2;
    int shift4 = shiftBit * 3;
    for( int i = 0 ; i < listSize ; i+=4 ){
      if( listSize == i + 1 ){
        int diff1 = diffList.get(i).intValue();
        out.write( diff1 );
      }
      else if( listSize == i + 2 ){
        int diff1 = diffList.get(i).intValue();
        int diff2 = diffList.get(i+1).intValue() << shift2;
        out.write( diff1 + diff2 );
      }
      else if( listSize == i + 3 ){
        int diff1 = diffList.get(i).intValue();
        int diff2 = diffList.get(i+1).intValue() << shift2;
        int diff3 = diffList.get(i+2).intValue() << shift3;
        out.write( diff1 + diff2 + diff3 );
      }
      else{
        int diff1 = diffList.get(i).intValue();
        int diff2 = diffList.get(i+1).intValue() << shift2;
        int diff3 = diffList.get(i+2).intValue() << shift3;
        int diff4 = diffList.get(i+3).intValue() << shift4;
        out.write( diff1 + diff2 + diff3 + diff4 );
      }
    }

  }

  public static byte[] compressInteger2( final List<Integer> dumpTarget )throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write( getBitNumber( CompressBit.COMPRESS_2 ) );
    if( dumpTarget == null || dumpTarget.isEmpty() ){
      return out.toByteArray();
    }

    int startIndex = dumpTarget.get(0).intValue();
    int before = dumpTarget.get(0).intValue();
    List<Integer> diffList = new ArrayList<Integer>();
    for( int i = 0 ; i < dumpTarget.size() ; i++ ){
      int target = dumpTarget.get(i).intValue();
      int diff = target - before;
      if( BIT_2 <= diff ){
        appendInteger2( out , startIndex , diffList );

        diffList.clear();
        startIndex = target;
        before = target;
        diff = 0;
      }
      else{
        before = target;
      }
      diffList.add( Integer.valueOf( diff ) );
    }
    appendInteger2( out , startIndex , diffList );

    return out.toByteArray();
  }

  private static List<Integer> decompressInteger4( final byte[] buffer , final int start , final int length ){
    List<Integer> result = new ArrayList<Integer>();
    int offset = start+1;
    while( offset < ( start + length ) ){
      int diffIndex = readInt( buffer , offset , 4 );
      offset += 4;
      int size = readInt( buffer , offset , 4 );
      offset += 4;
      for( int i = 0 ; i < size ; i+=2 ){
        int compressInt = buffer[offset] & 255;
        int diff1 = compressInt & 15;
        int diff2 = compressInt >>> getBitNumber( CompressBit.COMPRESS_4 );
        diffIndex += diff1;
        result.add( diffIndex );
        if( size != i + 1 ){
          diffIndex += diff2;
          result.add( diffIndex );
        }
        offset++;
      }
    }
    return result;
  }

  private static void appendInteger4( final OutputStream out , final int startIndex , final List<Integer> diffList ) throws IOException{
    int length = diffList.size();
    writeInt( out , startIndex );
    writeInt( out , length );

    int listSize = diffList.size();
    for( int i = 0 ; i < listSize ; i+=2 ){
      if( listSize == i + 1 ){
        int diff1 = diffList.get(i).intValue();
        out.write( diff1 );
      }
      else{ 
        int diff1 = diffList.get(i).intValue();
        int diff2 = diffList.get(i+1).intValue();
        out.write( ( diff1 + ( diff2 << ( getBitNumber( CompressBit.COMPRESS_4 ) ) ) ) );
      }
    }

  }

  public static byte[] compressInteger4( final List<Integer> dumpTarget )throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write( getBitNumber( CompressBit.COMPRESS_4 ) );
    if( dumpTarget == null || dumpTarget.isEmpty() ){
      return out.toByteArray();
    }

    int startIndex = dumpTarget.get(0).intValue();
    int before = dumpTarget.get(0).intValue();
    List<Integer> diffList = new ArrayList<Integer>();
    for( int i = 0 ; i < dumpTarget.size() ; i++ ){
      int target = dumpTarget.get(i).intValue();
      int diff = target - before;
      if( BIT_4 <= diff ){
        appendInteger4( out , startIndex , diffList );

        diffList.clear();
        startIndex = target;
        before = target;
        diff = 0;
      }
      else{
        before = target;
      }
      diffList.add( Integer.valueOf( diff ) );
    }
    appendInteger4( out , startIndex , diffList );

    return out.toByteArray();
  }

  private static List<Integer> decompressInteger8( final byte[] buffer , final int start , final int length ){
    List<Integer> result = new ArrayList<Integer>();
    int offset = start+1;
    while( offset < ( start + length ) ){
      int diffIndex = readInt( buffer , offset , 4 ); 
      offset += 4;
      int size = readInt( buffer , offset , 4 ); 
      offset += 4;
      for( int i = 0 ; i < size ; i++ ){
        diffIndex = diffIndex + ( buffer[offset] & 255 ); 
        result.add( diffIndex );
        offset+=1;
      }
    }
    return result;
  }

  private static void appendInteger8( final OutputStream out , final int startIndex , final List<Integer> diffList ) throws IOException{
    int length = diffList.size();
    writeInt( out , startIndex );
    writeInt( out , length );

    for( int i = 0 ; i < diffList.size() ; i++ ){
      out.write( diffList.get(i).intValue() );
    }

  }

  public static byte[] compressInteger8( final List<Integer> dumpTarget )throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write( getBitNumber( CompressBit.COMPRESS_8 ) );
    if( dumpTarget == null || dumpTarget.isEmpty() ){
      return out.toByteArray();
    }

    int startIndex = dumpTarget.get(0).intValue();
    int before = dumpTarget.get(0).intValue();
    List<Integer> diffList = new ArrayList<Integer>();
    for( int i = 0 ; i < dumpTarget.size() ; i++ ){
      int target = dumpTarget.get(i).intValue();
      int diff = target - before;
      if( BYTE_1 <= diff ){
        appendInteger8( out , startIndex , diffList );

        diffList.clear();
        startIndex = target;
        before = target;
        diff = 0;
      }
      else{
        before = target;
      }
      diffList.add( Integer.valueOf( diff ) );
    }
    appendInteger8( out , startIndex , diffList );

    return out.toByteArray();
  }

  private static List<Integer> decompressInteger16( final byte[] buffer , final int start , final int length ){
    List<Integer> result = new ArrayList<Integer>();
    int offset = start+1;
    while( offset < ( start + length ) ){
      int diffIndex = readInt( buffer , offset , 4 );
      offset += 4;
      int size = readInt( buffer , offset , 4 );
      offset += 4;
      for( int i = 0 ; i < size ; i++ ){
        diffIndex = diffIndex + ( ( buffer[offset] & 255 ) << 8 ) + ( buffer[offset+1] & 255 );
        result.add( diffIndex );
        offset+=2;
      }
    }
    return result;
  }

  private static void appendInteger16( final OutputStream out , final int startIndex , final List<Integer> diffList ) throws IOException{
    int length = diffList.size();
    writeInt( out , startIndex );
    writeInt( out , length );

    for( int i = 0 ; i < diffList.size() ; i++ ){
      int target = diffList.get(i).intValue();
      out.write( target >>> 8 );
      out.write( target );
    }

  }

  public static byte[] compressInteger16( final List<Integer> dumpTarget )throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write( getBitNumber( CompressBit.COMPRESS_16 ) );
    if( dumpTarget == null || dumpTarget.isEmpty() ){
      return out.toByteArray();
    }

    int startIndex = dumpTarget.get(0).intValue();
    int before = dumpTarget.get(0).intValue();
    List<Integer> diffList = new ArrayList<Integer>();
    for( int i = 0 ; i < dumpTarget.size() ; i++ ){
      int target = dumpTarget.get(i).intValue();
      int diff = target - before;
      if( BYTE_2 <= diff ){
        appendInteger16( out , startIndex , diffList );

        diffList.clear();
        startIndex = target;
        before = target;
        diff = 0;
      }
      else{
        before = target;
      }
      diffList.add( Integer.valueOf( diff ) );
    }
    appendInteger16( out , startIndex , diffList );

    return out.toByteArray();
  }

  public static double intToDouble( final int target ){
    return Integer.valueOf( target ).doubleValue();
  }
 
  public static int getBitToInt( final CompressBit compressBit ){
    switch( compressBit ){
      case COMPRESS_16:
        return BYTE_2;
      case COMPRESS_8:
        return BYTE_1;
      case COMPRESS_4:
        return BIT_4;
      case COMPRESS_2:
        return BIT_2;
      default:
        return BIT_1;
    }
  }

  public static int getBitNumber( final CompressBit compressBit ){
    switch( compressBit ){
      case COMPRESS_16:
        return 16;
      case COMPRESS_8:
        return 8;
      case COMPRESS_4:
        return 4;
      case COMPRESS_2:
        return 2;
      default:
        return 1;
    }
  }

  public static CompressBit getBitType( final byte bitNumber ){
    switch( bitNumber ){
      case 16:
        return CompressBit.COMPRESS_16;
      case 8:
        return CompressBit.COMPRESS_8;
      case 4:
        return CompressBit.COMPRESS_4;
      case 2:
        return CompressBit.COMPRESS_2;
      case 1:
        return CompressBit.COMPRESS_1;
      default:
        return CompressBit.DUMP_INTEGER;
    }
  }

  public static List<Integer> getIntegerList( final byte[] indexBinary , final int start , final int length ){
    CompressBit compressType = getBitType( indexBinary[start] );
    switch( compressType ){
      case COMPRESS_16:
        return decompressInteger16( indexBinary , start , length );
      case COMPRESS_8:
        return decompressInteger8( indexBinary , start , length );
      case COMPRESS_4:
        return decompressInteger4( indexBinary , start , length );
      case COMPRESS_2:
        return decompressInteger2( indexBinary , start , length );
      case COMPRESS_1:
        return decompressInteger1( indexBinary , start , length );
      default:
        return getFromDumpInteger( indexBinary , start , length );
    }
  }

}
