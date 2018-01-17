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
package jp.co.yahoo.dataplatform.mds.compressor;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;

import jp.co.yahoo.dataplatform.config.FindClass;

public final class FindCompressor{

  private final static FindCompressor OBJ = new FindCompressor();
  private final static Object LOCK = new Object();
  private final static Map<String,ICompressor> CACHE = new HashMap<String,ICompressor>();

  private FindCompressor(){}

  public static ICompressor get( final String target ) throws IOException{
    if( CACHE.containsKey( target ) ){
      return CACHE.get( target );
    }
    Object obj = FindClass.getObject( target , true , OBJ.getClass().getClassLoader() );
    if( ! ( obj instanceof ICompressor ) ){
      throw new IOException( "Invalid ICompressor class : " + target );
    }
    if( ! CACHE.containsKey( target ) ){
      synchronized( LOCK ){
        if( ! CACHE.containsKey( target ) ){
          CACHE.put( target , (ICompressor)obj );
        }
      }
    }
    return (ICompressor)obj;
  }

}
