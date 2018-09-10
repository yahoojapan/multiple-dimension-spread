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
package jp.co.yahoo.dataplatform.mds.tools;

import java.io.IOException;
import java.io.OutputStream;

import jp.co.yahoo.dataplatform.schema.formatter.IStreamWriter;

public final class StreamWriterFactory{

  private void StreamWriterFactory(){}

  public static IStreamWriter create( final OutputStream out , final String format , final String schema ) throws IOException{
    if( "json".equals( format ) ){
      return new LineJsonStreamWriter( out );
    }
    else{
      throw new IOException( String.format( "Unknown format %s ." , format ) );
    }
  }

}
