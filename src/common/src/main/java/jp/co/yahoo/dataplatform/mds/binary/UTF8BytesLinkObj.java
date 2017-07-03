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

import jp.co.yahoo.dataplatform.schema.objects.StringObj;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

public class UTF8BytesLinkObj extends StringObj implements IBytesLink {

  private final int start;
  private final int length;
  private final byte[] value;

  public UTF8BytesLinkObj( final byte[] data , final int start , final int length ){
    this.value = data;
    this.start = start;
    this.length = length;
  }

  @Override
  public byte[] getLinkBytes(){
    return value;
  }

  @Override
  public int getStart(){
    return start;
  }

  @Override
  public int getLength(){
    return length;
  }

  @Override
  public String getString() throws IOException{
    return new String( value , start , length , "UTF-8" );
  }

  @Override
  public byte[] getBytes() throws IOException{
    byte[] result = new byte[length];
    System.arraycopy( value , start , result , 0 , length );
    return result;
  }

  @Override
  public void setString( final String data ) throws IOException{
    throw new IOException( "Unsupported set method." );
  }

  @Override
  public void setBytes( final byte[] data ) throws IOException{
    throw new IOException( "Unsupported set method." );
  }

  @Override
  public void setBytes( final byte[] data , final int start , final int length ) throws IOException{
    throw new IOException( "Unsupported set method." );
  }

  @Override
  public void setByte( final byte data ) throws IOException{
    throw new IOException( "Unsupported set method." );
  }

  @Override
  public void setShort( final short data ) throws IOException{
    throw new IOException( "Unsupported set method." );
  }

  @Override
  public void setInt( final int data ) throws IOException{
    throw new IOException( "Unsupported set method." );
  }

  @Override
  public void setLong( final long data ) throws IOException{
    throw new IOException( "Unsupported set method." );
  }

  @Override
  public void setFloat( final float data ) throws IOException{
    throw new IOException( "Unsupported set method." );
  }

  @Override
  public void setDouble( final double data ) throws IOException{
    throw new IOException( "Unsupported set method." );
  }

  @Override
  public void setBoolean( final boolean data ) throws IOException{
    throw new IOException( "Unsupported set method." );
  }

  @Override
  public void set( final PrimitiveObject data ) throws IOException{
    throw new IOException( "Unsupported set method." );
  }

  @Override
  public void clear() throws IOException{
    throw new IOException( "Unsupported set method." );
  }

}
