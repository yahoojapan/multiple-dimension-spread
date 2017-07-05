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
package jp.co.yahoo.dataplatform.mds.inmemory;

import java.io.IOException;

import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

public interface IMemoryAllocator{

  void setNull( final int index ) throws IOException;

  void setBoolean( final int index , final boolean value ) throws IOException;

  void setByte( final int index , final byte value ) throws IOException; 

  void setShort( final int index , final short value ) throws IOException;

  void setInteger( final int index , final int value ) throws IOException;

  void setLong( final int index , final long value ) throws IOException;

  void setFloat( final int index , final float value ) throws IOException;

  void setDouble( final int index , final double value ) throws IOException;

  void setBytes( final int index , final byte[] value ) throws IOException;

  void setBytes( final int index , final byte[] value , final int start , final int length ) throws IOException;

  void setString( final int index , final String value ) throws IOException;

  void setString( final int index , final char[] value ) throws IOException;

  void setString( final int index , final char[] value , final int start , final int length ) throws IOException;

  void setArrayIndex( final int index , final int start , final int length ) throws IOException;

  IMemoryAllocator getChild( final String columnName , final ColumnType type ) throws IOException;

}
