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
package jp.co.yahoo.dataplatform.mds.util.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;
import jp.co.yahoo.dataplatform.schema.objects.IntegerObj;
import jp.co.yahoo.dataplatform.schema.objects.LongObj;

import jp.co.yahoo.dataplatform.mds.util.io.IReadSupporter;
import jp.co.yahoo.dataplatform.mds.util.io.IWriteSupporter;
import jp.co.yahoo.dataplatform.mds.util.io.unsafe.ByteBufferSupporterFactory;

public final class NumberToBinaryUtils{

  public static final int INT_BYTE_MAX_LENGTH = 0xFF;
  public static final int INT_SHORT_MAX_LENGTH = 0xFFFF;

  public static final long LONG_BYTE_MAX_LENGTH = 0xFFL;
  public static final long LONG_SHORT_MAX_LENGTH = 0xFFFFL;
  public static final long LONG_INT_MAX_LENGTH = 0xFFFFFFFFL;

  public static final int HEADER_SIZE = 2;

  public static final byte HEADER_8 = (byte)8;
  public static final byte HEADER_7 = (byte)7;
  public static final byte HEADER_6 = (byte)6;
  public static final byte HEADER_5 = (byte)5;
  public static final byte HEADER_4 = (byte)4;
  public static final byte HEADER_3 = (byte)3;
  public static final byte HEADER_2 = (byte)2;
  public static final byte HEADER_1 = (byte)1;
  public static final byte HEADER_0 = (byte)0;

  public static final long LONG_BIT_0 = 0L;
  public static final long LONG_BIT_1 = 1L;
  public static final long LONG_BIT_9 = 1L << 8;
  public static final long LONG_BIT_17 = 1L << 16;
  public static final long LONG_BIT_25 = 1L << 24;
  public static final long LONG_BIT_33 = 1L << 32;
  public static final long LONG_BIT_41 = 1L << 40;
  public static final long LONG_BIT_49 = 1L << 48;
  public static final long LONG_BIT_57 = 1L << 56;

  public static final int INT_BIT_0 = 0;
  public static final int INT_BIT_1 = 1;
  public static final int INT_BIT_9 = 1 << 8;
  public static final int INT_BIT_17 = 1 << 16;
  public static final int INT_BIT_25 = 1 << 24;

  public static class WriteSupporter_7 implements IWriteSupporter{

    private IWriteSupporter byteSupporter;
    private IWriteSupporter shortSupporter;
    private IWriteSupporter intSupporter;

    public WriteSupporter_7( final IWriteSupporter byteSupporter , final IWriteSupporter shortSupporter , final IWriteSupporter intSupporter ){
      this.byteSupporter = byteSupporter;
      this.shortSupporter = shortSupporter;
      this.intSupporter = intSupporter;
    }

    @Override
    public void putLong( final long value ){
      byteSupporter.putByte( (byte)( value >> 48 ) );
      shortSupporter.putShort( (short)( value >> 32 ) );
      intSupporter.putInt( (int)value );
    }

  }

  public static class ReadSupporter_7 implements IReadSupporter{

    private IReadSupporter byteSupporter;
    private IReadSupporter shortSupporter;
    private IReadSupporter intSupporter;

    public ReadSupporter_7( final IReadSupporter byteSupporter , final IReadSupporter shortSupporter , final IReadSupporter intSupporter ){
      this.byteSupporter = byteSupporter;
      this.shortSupporter = shortSupporter;
      this.intSupporter = intSupporter;
    }

    @Override
    public  long getLong(){
      return (
            ( getUnsignedByteToLong( byteSupporter.getByte() ) << 48 )
          + ( getUnsignedShortToLong( shortSupporter.getShort() ) << 32 )
          + getUnsignedIntToLong( intSupporter.getInt() )
      );
    }

  }

  public static class WriteSupporter_6 implements IWriteSupporter{

    private IWriteSupporter shortSupporter;
    private IWriteSupporter intSupporter;

    public WriteSupporter_6( final IWriteSupporter shortSupporter , final IWriteSupporter intSupporter ){
      this.shortSupporter = shortSupporter;
      this.intSupporter = intSupporter;
    }

    @Override
    public void putLong( final long value ){
      shortSupporter.putShort( (short)( value >> 32 ) );
      intSupporter.putInt( (int)value );
    }

  }

  public static class ReadSupporter_6 implements IReadSupporter{

    private IReadSupporter shortSupporter;
    private IReadSupporter intSupporter;

    public ReadSupporter_6( final IReadSupporter shortSupporter , final IReadSupporter intSupporter ){
      this.shortSupporter = shortSupporter;
      this.intSupporter = intSupporter;
    }

    @Override
    public  long getLong(){
      return (
          + ( getUnsignedShortToLong( shortSupporter.getShort() ) << 32 )
          + getUnsignedIntToLong( intSupporter.getInt() )
      );
    }

  }

  public static class WriteSupporter_5 implements IWriteSupporter{

    private IWriteSupporter byteSupporter;
    private IWriteSupporter intSupporter;

    public WriteSupporter_5( final IWriteSupporter byteSupporter , final IWriteSupporter intSupporter ){
      this.byteSupporter = byteSupporter;
      this.intSupporter = intSupporter;
    }

    @Override
    public void putLong( final long value ){
      byteSupporter.putByte( (byte)( value >> 32 ) );
      intSupporter.putInt( (int)value );
    }

  }

  public static class ReadSupporter_5 implements IReadSupporter{

    private IReadSupporter byteSupporter;
    private IReadSupporter intSupporter;

    public ReadSupporter_5( final IReadSupporter byteSupporter , final IReadSupporter intSupporter ){
      this.byteSupporter = byteSupporter;
      this.intSupporter = intSupporter;
    }

    @Override
    public  long getLong(){
      return (
            ( getUnsignedByteToLong( byteSupporter.getByte() ) << 32 )
          + getUnsignedIntToLong( intSupporter.getInt() )
      );
    }

  }

  public static class WriteSupporter_4 implements IWriteSupporter{

    private IWriteSupporter intSupporter;

    public WriteSupporter_4( final IWriteSupporter intSupporter ){
      this.intSupporter = intSupporter;
    }

    @Override
    public void putInt( final int value ){
      intSupporter.putInt( value );
    }

    @Override
    public void putLong( final long value ){
      intSupporter.putInt( (int)value );
    }

  }

  public static class ReadSupporter_4 implements IReadSupporter{

    private IReadSupporter intSupporter;

    public ReadSupporter_4( final IReadSupporter intSupporter ){
      this.intSupporter = intSupporter;
    }

    @Override
    public int getInt(){
      return intSupporter.getInt();
    }

    @Override
    public  long getLong(){
      return getUnsignedIntToLong( intSupporter.getInt() );
    }

  }

  public static class WriteSupporter_3 implements IWriteSupporter{

    private IWriteSupporter byteSupporter;
    private IWriteSupporter shortSupporter;

    public WriteSupporter_3( final IWriteSupporter byteSupporter , final IWriteSupporter shortSupporter ){
      this.byteSupporter = byteSupporter;
      this.shortSupporter = shortSupporter;
    }

    @Override
    public void putInt( final int value ){
      byteSupporter.putByte( (byte)( value >> 16 ) );
      shortSupporter.putShort( (short)value );
    }

    @Override
    public void putLong( final long value ){
      byteSupporter.putByte( (byte)( value >> 16 ) );
      shortSupporter.putShort( (short)value );
    }

  }

  public static class ReadSupporter_3 implements IReadSupporter{

    private IReadSupporter byteSupporter;
    private IReadSupporter shortSupporter;

    public ReadSupporter_3( final IReadSupporter byteSupporter , final IReadSupporter shortSupporter ){
      this.byteSupporter = byteSupporter;
      this.shortSupporter = shortSupporter;
    }

    @Override
    public int getInt(){
      return ( 
            ( getUnsignedByteToInt( byteSupporter.getByte() ) << 16 )
          + getUnsignedShortToInt( shortSupporter.getShort() )
      );
    }

    @Override
    public  long getLong(){
      return (
            ( getUnsignedByteToLong( byteSupporter.getByte() ) << 16 )
          + getUnsignedShortToLong( shortSupporter.getShort() )
      );
    }

  }

  public static class WriteSupporter_2 implements IWriteSupporter{

    private IWriteSupporter shortSupporter;

    public WriteSupporter_2( final IWriteSupporter shortSupporter ){
      this.shortSupporter = shortSupporter;
    }

    @Override
    public void putInt( final int value ){
      shortSupporter.putShort( (short)value );
    }

    @Override
    public void putLong( final long value ){
      shortSupporter.putShort( (short)value );
    }

  }

  public static class ReadSupporter_2 implements IReadSupporter{

    IReadSupporter shortSupporter;

    public ReadSupporter_2( final IReadSupporter shortSupporter ){
      this.shortSupporter = shortSupporter;
    }

    @Override
    public int getInt(){
      return getUnsignedShortToInt( shortSupporter.getShort() );
    }

    @Override
    public long getLong(){
      return getUnsignedShortToLong( shortSupporter.getShort() );
    }

  }

  public static class WriteSupporter_1 implements IWriteSupporter{

    private IWriteSupporter byteSupporter;

    public WriteSupporter_1( final IWriteSupporter byteSupporter ){
      this.byteSupporter = byteSupporter;
    }

    @Override
    public void putInt( final int value ){
      byteSupporter.putByte( (byte)value );
    }

    @Override
    public void putLong( final long value ){
      byteSupporter.putByte( (byte)value );
    }

  }

  public static class ReadSupporter_1 implements IReadSupporter{

    IReadSupporter byteSupporter;

    public ReadSupporter_1( final IReadSupporter byteSupporter ){
      this.byteSupporter = byteSupporter;
    }

    @Override
    public int getInt(){
      return getUnsignedByteToInt( byteSupporter.getByte() );
    }

  }

  public static class WriteSupporter_0 implements IWriteSupporter{

    @Override
    public void putInt( final int value ){
    }

    @Override
    public void putLong( final long value ){
    }

  }

  public static class ReadSupporter_0 implements IReadSupporter{

    @Override
    public int getInt(){
      return 0;
    }

    @Override
    public long getLong(){
      return 0L;
    }

  }

  public interface IIntConverter{

    int calcBinarySize( final int rows );

    int getBaseBytes();

    IWriteSupporter toWriteSuppoter( final int rows , final byte[] buffer , final int start , final int length ) throws IOException;

    IReadSupporter toReadSupporter( final byte[] buffer , final int start , final int length ) throws IOException;

  }

  public static class IntConverter_4 implements IIntConverter{

    @Override
    public int calcBinarySize( final int rows ){
      return rows * Integer.BYTES + HEADER_SIZE;
    }

    @Override
    public int getBaseBytes() {
      return Integer.BYTES;
    }

    @Override
    public IWriteSupporter toWriteSuppoter( final int rows, final byte[] buffer , final int start , final int length ) throws IOException{
      ByteOrder order = ByteOrder.nativeOrder();
      byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

      buffer[start] = HEADER_4;
      buffer[start+1] = byteOrderByte;

      int intStart = start + HEADER_SIZE;
      int intLength = length - HEADER_SIZE;
      return ByteBufferSupporterFactory.createWriteSupporter( buffer , intStart , intLength , order );
    }

    @Override
    public IReadSupporter toReadSupporter( byte[] buffer , final int start , final int length ) throws IOException{
      if( buffer[start] != HEADER_4 ){
        throw new IOException( "Invalid binary." );
      }
      ByteOrder order = buffer[start+1] == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      int intStart = start + HEADER_SIZE;
      int intLength = length - HEADER_SIZE;
      return ByteBufferSupporterFactory.createReadSupporter( buffer , intStart , intLength , order );
    }

  }

  public static class IntConverter_3 implements IIntConverter{

    @Override
    public int calcBinarySize( final int rows ){
      int byteLength = Byte.BYTES * rows;
      int shortLength = Short.BYTES * rows;
      return shortLength + byteLength + HEADER_SIZE;
    }

    @Override
    public int getBaseBytes() {
      return Byte.BYTES + Short.BYTES;
    }

    @Override
    public IWriteSupporter toWriteSuppoter( final int rows , final byte[] buffer , final int start , final int length ) throws IOException{
      int byteLength = Byte.BYTES * rows;
      int shortLength = Short.BYTES * rows;

      ByteOrder order = ByteOrder.nativeOrder();
      byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

      buffer[start] = HEADER_3;
      buffer[start+1] = byteOrderByte;

      int byteStart = start + HEADER_SIZE;
      int shortStart = byteStart + byteLength;

      IWriteSupporter byteSupporter = ByteBufferSupporterFactory.createWriteSupporter( buffer , byteStart , byteLength , order );
      IWriteSupporter shortSupporter = ByteBufferSupporterFactory.createWriteSupporter( buffer , shortStart , shortLength , order );

      return new WriteSupporter_3( byteSupporter , shortSupporter );
    }

    @Override
    public IReadSupporter toReadSupporter( byte[] buffer , final int start , final int length ) throws IOException{
      if( buffer[start] != HEADER_3 ){
        throw new IOException( "Invalid binary." );
      }
      ByteOrder order = buffer[start+1] == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      int rows = ( length - HEADER_SIZE ) / ( Byte.BYTES + Short.BYTES );
      int[] result = new int[rows];
      int byteLength = Byte.BYTES * result.length;
      int shortLength = Short.BYTES * result.length;

      int byteStart = start + HEADER_SIZE;
      int shortStart = byteStart + byteLength;

      IReadSupporter byteSupporter = ByteBufferSupporterFactory.createReadSupporter( buffer , byteStart , byteLength , order );
      IReadSupporter shortSupporter = ByteBufferSupporterFactory.createReadSupporter( buffer , shortStart , shortLength , order );

      return new ReadSupporter_3( byteSupporter , shortSupporter );
    }

  }

  public static class IntConverter_2 implements IIntConverter{

    @Override
    public int calcBinarySize( final int rows ){
      int shortLength = Short.BYTES * rows;
      return shortLength + HEADER_SIZE;
    }

    @Override
    public int getBaseBytes() {
      return Short.BYTES;
    }

    @Override
    public IWriteSupporter toWriteSuppoter( final int rows , final byte[] buffer , final int start , final int length ) throws IOException{
      int shortLength = Short.BYTES * rows;

      ByteOrder order = ByteOrder.nativeOrder();
      byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

      buffer[start] = HEADER_2;
      buffer[start+1] = byteOrderByte;

      int shortStart = start + HEADER_SIZE;

      return new WriteSupporter_2( ByteBufferSupporterFactory.createWriteSupporter( buffer , shortStart , shortLength , order ) );
    }

    @Override
    public IReadSupporter toReadSupporter( byte[] buffer , final int start , final int length ) throws IOException{
      if( buffer[start] != HEADER_2 ){
        throw new IOException( "Invalid binary." );
      }
      ByteOrder order = buffer[start+1] == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      int rows = ( length - HEADER_SIZE ) / Short.BYTES;
      int[] result = new int[rows];
      int shortLength = Short.BYTES * result.length;

      int shortStart = start + HEADER_SIZE;

      return new ReadSupporter_2( ByteBufferSupporterFactory.createReadSupporter( buffer , shortStart , shortLength , order ) );
    }

  }

  public static class IntConverter_1 implements IIntConverter{

    @Override
    public int calcBinarySize( final int rows ){
      int byteLength = Byte.BYTES * rows;
      return byteLength + HEADER_SIZE;
    }

    @Override
    public int getBaseBytes() {
      return Byte.BYTES;
    }

    @Override
    public IWriteSupporter toWriteSuppoter( final int rows , final byte[] buffer , final int start , final int length ) throws IOException{
      int byteLength = Byte.BYTES * rows;

      ByteOrder order = ByteOrder.nativeOrder();
      byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

      buffer[start] = HEADER_1;
      buffer[start+1] = byteOrderByte;

      int byteStart = start + HEADER_SIZE;

      return new WriteSupporter_2(  ByteBufferSupporterFactory.createWriteSupporter( buffer , byteStart , byteLength , order ) );
    }

    @Override
    public IReadSupporter toReadSupporter( byte[] buffer , final int start , final int length ) throws IOException{
      if( buffer[start] != HEADER_1 ){
        throw new IOException( "Invalid binary." );
      }
      ByteOrder order = buffer[start+1] == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      int rows = ( length - HEADER_SIZE ) / Byte.BYTES;
      int[] result = new int[rows];
      int byteLength = Byte.BYTES * result.length;

      int byteStart = start + HEADER_SIZE;

      return new ReadSupporter_1( ByteBufferSupporterFactory.createReadSupporter( buffer , byteStart , byteLength , order ) );
    }

  }

  public static class IntConverter_0 implements IIntConverter{

    @Override
    public int calcBinarySize( final int rows ){
      return HEADER_SIZE;
    }

    @Override
    public int getBaseBytes() {
      return 0;
    }

    @Override
    public IWriteSupporter toWriteSuppoter( final int rows , final byte[] buffer , final int start , final int length ) throws IOException{
      ByteOrder order = ByteOrder.nativeOrder();
      byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

      buffer[start] = HEADER_0;
      buffer[start+1] = byteOrderByte;

      return new WriteSupporter_0();
    }

    @Override
    public IReadSupporter toReadSupporter( byte[] buffer , final int start , final int length ) throws IOException{
      if( buffer[start] != HEADER_0 ){
        throw new IOException( "Invalid binary." );
      }
      return new ReadSupporter_0();
    }

  }

  public interface ILongConverter{

    int calcBinarySize( final int rows );

    IWriteSupporter toWriteSuppoter( final int rows , final byte[] buffer , final int start , final int length ) throws IOException;

    IReadSupporter toReadSupporter( final byte[] buffer , final int start , final int length ) throws IOException;

  }

  public static class LongConverter_8 implements ILongConverter{

    @Override
    public int calcBinarySize( final int rows ){
      return rows * Long.BYTES + HEADER_SIZE;
    }

    @Override
    public IWriteSupporter toWriteSuppoter( final int rows , final byte[] buffer , final int start , final int length ) throws IOException{
      ByteOrder order = ByteOrder.nativeOrder();
      byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

      buffer[start] = HEADER_8;
      buffer[start+1] = byteOrderByte;

      int longStart = start + HEADER_SIZE;
      int longLength = length - HEADER_SIZE;
      return ByteBufferSupporterFactory.createWriteSupporter( buffer , longStart , longLength , order );
    }

    @Override
    public IReadSupporter toReadSupporter( byte[] buffer , final int start , final int length ) throws IOException{
      if( buffer[start] != HEADER_8 ){
        throw new IOException( "Invalid binary." );
      }
      ByteOrder order = buffer[start+1] == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      int longStart = start + HEADER_SIZE;
      int longLength = length - HEADER_SIZE;
      return ByteBufferSupporterFactory.createReadSupporter( buffer , longStart , longLength , order );
    }

  }

  public static class LongConverter_7 implements ILongConverter{

    @Override
    public int calcBinarySize( final int rows ){
      int byteLength = Byte.BYTES * rows;
      int shortLength = Short.BYTES * rows;
      int intLength = Integer.BYTES * rows;
      return intLength + shortLength + byteLength + HEADER_SIZE;
    }

    @Override
    public IWriteSupporter toWriteSuppoter( final int rows , final byte[] buffer , final int start , final int length ) throws IOException{
      int byteLength = Byte.BYTES * rows;
      int shortLength = Short.BYTES * rows;
      int intLength = Integer.BYTES * rows;

      ByteOrder order = ByteOrder.nativeOrder();
      byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

      buffer[start] = HEADER_7;
      buffer[start+1] = byteOrderByte;

      int byteStart = start + HEADER_SIZE;
      int shortStart = byteStart + byteLength;
      int intStart = shortStart + shortLength;

      IWriteSupporter byteSupporter = ByteBufferSupporterFactory.createWriteSupporter( buffer , byteStart , byteLength , order );
      IWriteSupporter shortSupporter = ByteBufferSupporterFactory.createWriteSupporter( buffer , shortStart , shortLength , order );
      IWriteSupporter intSupporter = ByteBufferSupporterFactory.createWriteSupporter( buffer , intStart , intLength , order );

      return new WriteSupporter_7( byteSupporter , shortSupporter , intSupporter );
    }

    @Override
    public IReadSupporter toReadSupporter( byte[] buffer , final int start , final int length ) throws IOException{
      if( buffer[start] != HEADER_7 ){
        throw new IOException( "Invalid binary." );
      }
      ByteOrder order = buffer[start+1] == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      int rows = ( length - HEADER_SIZE ) / ( Byte.BYTES + Short.BYTES + Integer.BYTES );
      long[] result = new long[rows];
      int byteLength = Byte.BYTES * result.length;
      int shortLength = Short.BYTES * result.length;
      int intLength = Integer.BYTES * result.length;

      int byteStart = start + HEADER_SIZE;
      int shortStart = byteStart + byteLength;
      int intStart = shortStart + shortLength;

      IReadSupporter byteSupporter = ByteBufferSupporterFactory.createReadSupporter( buffer , byteStart , byteLength , order );
      IReadSupporter shortSupporter = ByteBufferSupporterFactory.createReadSupporter( buffer , shortStart , shortLength , order );
      IReadSupporter intSupporter = ByteBufferSupporterFactory.createReadSupporter( buffer , intStart , intLength , order );
      return new ReadSupporter_7( byteSupporter , shortSupporter , intSupporter );
    }

  }

  public static class LongConverter_6 implements ILongConverter{

    @Override
    public int calcBinarySize( final int rows ){
      int shortLength = Short.BYTES * rows;
      int intLength = Integer.BYTES * rows;
      return intLength + shortLength + HEADER_SIZE;
    }

    @Override
    public IWriteSupporter toWriteSuppoter( final int rows , final byte[] buffer , final int start , final int length ) throws IOException{
      int shortLength = Short.BYTES * rows;
      int intLength = Integer.BYTES * rows;

      ByteOrder order = ByteOrder.nativeOrder();
      byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

      buffer[start] = HEADER_6;
      buffer[start+1] = byteOrderByte;

      int shortStart = start + HEADER_SIZE;
      int intStart = shortStart + shortLength;

      IWriteSupporter shortSupporter = ByteBufferSupporterFactory.createWriteSupporter( buffer , shortStart , shortLength , order );
      IWriteSupporter intSupporter = ByteBufferSupporterFactory.createWriteSupporter( buffer , intStart , intLength , order );
      return new WriteSupporter_6( shortSupporter , intSupporter );
    }

    @Override
    public IReadSupporter toReadSupporter( byte[] buffer , final int start , final int length ) throws IOException{
      if( buffer[start] != HEADER_6 ){
        throw new IOException( "Invalid binary." );
      }
      ByteOrder order = buffer[start+1] == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      int rows = ( length - HEADER_SIZE ) / ( Short.BYTES + Integer.BYTES );
      long[] result = new long[rows];
      int shortLength = Short.BYTES * result.length;
      int intLength = Integer.BYTES * result.length;

      int shortStart = start + HEADER_SIZE;
      int intStart = shortStart + shortLength;

      IReadSupporter shortSupporter = ByteBufferSupporterFactory.createReadSupporter( buffer , shortStart , shortLength , order );
      IReadSupporter intSupporter = ByteBufferSupporterFactory.createReadSupporter( buffer , intStart , intLength , order );

      return new ReadSupporter_6( shortSupporter , intSupporter );
    }

  }

  public static class LongConverter_5 implements ILongConverter{

    @Override
    public int calcBinarySize( final int rows ){
      int byteLength = Byte.BYTES * rows;
      int intLength = Integer.BYTES * rows;
      return intLength + byteLength + HEADER_SIZE;
    }

    @Override
    public IWriteSupporter toWriteSuppoter( final int rows , final byte[] buffer , final int start , final int length ) throws IOException{
      int byteLength = Byte.BYTES * rows;
      int intLength = Integer.BYTES * rows;

      ByteOrder order = ByteOrder.nativeOrder();
      byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

      buffer[start] = HEADER_5;
      buffer[start + 1] = byteOrderByte;

      int byteStart = start + HEADER_SIZE;
      int intStart = byteStart + byteLength;

      IWriteSupporter byteSupporter = ByteBufferSupporterFactory.createWriteSupporter( buffer , byteStart , byteLength , order );
      IWriteSupporter intSupporter = ByteBufferSupporterFactory.createWriteSupporter( buffer , intStart , intLength , order );
      return new WriteSupporter_5( byteSupporter , intSupporter );
    }

    @Override
    public IReadSupporter toReadSupporter( byte[] buffer , final int start , final int length ) throws IOException{
      if( buffer[start] != HEADER_5 ){
        throw new IOException( "Invalid binary." );
      }
      ByteOrder order = buffer[start+1] == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      int rows = ( length - HEADER_SIZE ) / ( Byte.BYTES + Integer.BYTES );
      long[] result = new long[rows];
      int byteLength = Byte.BYTES * result.length;
      int intLength = Integer.BYTES * result.length;

      int byteStart = start + HEADER_SIZE;
      int intStart = byteStart + byteLength;

      IReadSupporter byteSupporter = ByteBufferSupporterFactory.createReadSupporter( buffer , byteStart , byteLength , order );
      IReadSupporter intSupporter = ByteBufferSupporterFactory.createReadSupporter( buffer , intStart , intLength , order );

      return new ReadSupporter_5( byteSupporter , intSupporter );
    }

  }

  public static class LongConverter_4 implements ILongConverter{

    @Override
    public int calcBinarySize( final int rows ){
      int intLength = Integer.BYTES * rows;
      return intLength + HEADER_SIZE;
    }

    @Override
    public IWriteSupporter toWriteSuppoter( final int rows , final byte[]  buffer , final int start , final int length ) throws IOException{
      ByteOrder order = ByteOrder.nativeOrder();
      byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

      buffer[start] = HEADER_4;
      buffer[start+1] = byteOrderByte;

      int intStart = start + HEADER_SIZE;
      int intLength = buffer.length - HEADER_SIZE;
      return new WriteSupporter_4( ByteBufferSupporterFactory.createWriteSupporter( buffer , intStart , intLength , order ) );
    }

    @Override
    public IReadSupporter toReadSupporter( byte[] buffer , final int start , final int length ) throws IOException{
      if( buffer[start] != HEADER_4 ){
        throw new IOException( "Invalid binary." );
      }
      ByteOrder order = buffer[start+1] == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      int intStart = start + HEADER_SIZE;
      int intLength = length - HEADER_SIZE;
      return new ReadSupporter_4( ByteBufferSupporterFactory.createReadSupporter( buffer , intStart , intLength , order ) );
    }

  }

  public static class LongConverter_3 implements ILongConverter{

    @Override
    public int calcBinarySize( final int rows ){
      int byteLength = Byte.BYTES * rows;
      int shortLength = Short.BYTES * rows;
      return shortLength + byteLength + HEADER_SIZE;
    }

    @Override
    public IWriteSupporter toWriteSuppoter( final int rows , final byte[] buffer , final int start , final int length ) throws IOException{
      int byteLength = Byte.BYTES * rows;
      int shortLength = Short.BYTES * rows;

      ByteOrder order = ByteOrder.nativeOrder();
      byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

      buffer[start] = HEADER_3;
      buffer[start+1] = byteOrderByte;

      int byteStart = start + HEADER_SIZE;
      int shortStart = byteStart + byteLength;

      IWriteSupporter byteSupporter = ByteBufferSupporterFactory.createWriteSupporter( buffer , byteStart , byteLength , order );
      IWriteSupporter shortSupporter = ByteBufferSupporterFactory.createWriteSupporter( buffer , shortStart , shortLength , order );
      return new WriteSupporter_3( byteSupporter , shortSupporter );
    }

    @Override
    public IReadSupporter toReadSupporter( byte[] buffer , final int start , final int length ) throws IOException{
      if( buffer[start] != HEADER_3 ){
        throw new IOException( "Invalid binary." );
      }
      ByteOrder order = buffer[start+1] == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      int rows = ( length - HEADER_SIZE ) / ( Byte.BYTES + Short.BYTES );
      long[] result = new long[rows];
      int byteLength = Byte.BYTES * result.length;
      int shortLength = Short.BYTES * result.length;

      int byteStart = start + HEADER_SIZE;
      int shortStart = byteStart + byteLength;

      IReadSupporter byteSupporter = ByteBufferSupporterFactory.createReadSupporter( buffer , byteStart , byteLength , order );
      IReadSupporter shortSupporter = ByteBufferSupporterFactory.createReadSupporter( buffer , shortStart , shortLength , order );
      return new ReadSupporter_3( byteSupporter , shortSupporter );
    }

  }

  public static class LongConverter_2 implements ILongConverter{

    @Override
    public int calcBinarySize( final int rows ){
      int shortLength = Short.BYTES * rows;
      return shortLength + HEADER_SIZE;
    }

    @Override
    public IWriteSupporter toWriteSuppoter( final int rows , final byte[] buffer , final int start , final int length ) throws IOException{
      int shortLength = Short.BYTES * rows;

      ByteOrder order = ByteOrder.nativeOrder();
      byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

      buffer[start] = HEADER_2;
      buffer[start+1] = byteOrderByte;

      int shortStart = start + HEADER_SIZE;

      return new WriteSupporter_2( ByteBufferSupporterFactory.createWriteSupporter( buffer , shortStart , shortLength , order ) );
    }

    @Override
    public IReadSupporter toReadSupporter( byte[] buffer , final int start , final int length ) throws IOException{
      if( buffer[start] != HEADER_2 ){
        throw new IOException( "Invalid binary." );
      }
      ByteOrder order = buffer[start+1] == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      int rows = ( length - HEADER_SIZE ) / ( Short.BYTES );
      long[] result = new long[rows];
      int shortLength = Short.BYTES * result.length;

      int shortStart = start + HEADER_SIZE;

      return new ReadSupporter_2( ByteBufferSupporterFactory.createReadSupporter( buffer , shortStart , shortLength , order ) );
    }

  }

  public static class LongConverter_1 implements ILongConverter{

    @Override
    public int calcBinarySize( final int rows ){
      int byteLength = Byte.BYTES * rows;
      return byteLength + HEADER_SIZE;
    }

    @Override
    public IWriteSupporter toWriteSuppoter( final int rows , final byte[] buffer , final int start , final int length ) throws IOException{
      int byteLength = Byte.BYTES * rows;

      ByteOrder order = ByteOrder.nativeOrder();
      byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

      buffer[start] = HEADER_1;
      buffer[start+1] = byteOrderByte;

      int byteStart = start + HEADER_SIZE;

      return new WriteSupporter_1( ByteBufferSupporterFactory.createWriteSupporter( buffer , byteStart , byteLength , order ) );
    }

    @Override
    public IReadSupporter toReadSupporter( byte[] buffer , final int start , final int length ) throws IOException{
      if( buffer[start] != HEADER_1 ){
        throw new IOException( "Invalid binary." );
      }
      ByteOrder order = buffer[start+1] == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      int rows = ( length - HEADER_SIZE ) / ( Byte.BYTES );
      long[] result = new long[rows];
      int byteLength = Byte.BYTES * result.length;

      int byteStart = start + HEADER_SIZE;

      return new ReadSupporter_1( ByteBufferSupporterFactory.createReadSupporter( buffer , byteStart , byteLength , order ) );
    }

  }

  public static class LongConverter_0 implements ILongConverter{

    @Override
    public int calcBinarySize( final int rows ){
      return HEADER_SIZE;
    }

    @Override
    public IWriteSupporter toWriteSuppoter( final int rows , final byte[] buffer , final int start , final int length ) throws IOException{
      ByteOrder order = ByteOrder.nativeOrder();
      byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

      buffer[start] = HEADER_0;
      buffer[start+1] = byteOrderByte;

      return new WriteSupporter_0();
    }

    @Override
    public IReadSupporter toReadSupporter( byte[] buffer , final int start , final int length ) throws IOException{
      if( buffer[start] != HEADER_0 ){
        throw new IOException( "Invalid binary." );
      }
      return new ReadSupporter_0();
    }

  }

  public static long getUnsignedByteToLong( final byte target ){
    return (long)( target & 0xFFL );
  }

  public static long getUnsignedShortToLong( final short target ){
    return (long)( target & 0xFFFFL );
  }

  public static long getUnsignedIntToLong( final int target ){
    return (long)( target & 0xFFFFFFFFL );
  }

  public static int getUnsignedByteToInt( final byte target ){
    return target & 0xFF;
  }

  public static int getUnsignedShortToInt( final short target ){
    return target & 0xFFFF;
  }

  public static ILongConverter getLongConverter( final long min , final long max ){
    if( min < 0 ){
      return new LongConverter_8();
    }

    long highestBit = Long.highestOneBit( max );
    if( highestBit >= LONG_BIT_57 ){
      return new LongConverter_8();
    }
    else if( highestBit >= LONG_BIT_49 ){
      return new LongConverter_7();
    }
    else if( highestBit >= LONG_BIT_41 ){
      return new LongConverter_6();
    }
    else if( highestBit >= LONG_BIT_33 ){
      return new LongConverter_5();
    }
    else if( highestBit >= LONG_BIT_25 ){
      return new LongConverter_4();
    }
    else if( highestBit >= LONG_BIT_17 ){
      return new LongConverter_3();
    }
    else if( highestBit >= LONG_BIT_9 ){
      return new LongConverter_2();
    }
    else if( highestBit >= LONG_BIT_1 ){
      return new LongConverter_1();
    }
    else if( highestBit >= LONG_BIT_0 ){
      return new LongConverter_0();
    }
    else{
      return new LongConverter_8();
    }
  }

  public static IIntConverter getIntConverter( final int min , final int max ){
    if( min < 0 ){
      return new IntConverter_4();
    }

    int highestBit = Integer.highestOneBit( max );
    if( highestBit >= INT_BIT_25 ){
      return new IntConverter_4();
    }
    else if( highestBit >= INT_BIT_17 ){
      return new IntConverter_3();
    }
    else if( highestBit >= INT_BIT_9 ){
      return new IntConverter_2();
    }
    else if( highestBit >= INT_BIT_1 ){
      return new IntConverter_1();
    }
    else if( highestBit >= INT_BIT_0 ){
      return new IntConverter_0();
    }
    else{
      return new IntConverter_4();
    }
  }

}
