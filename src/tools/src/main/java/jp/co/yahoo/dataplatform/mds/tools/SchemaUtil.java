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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

import jp.co.yahoo.dataplatform.schema.design.IField;
import jp.co.yahoo.dataplatform.schema.design.HiveSchemaFactory;

public final class SchemaUtil{

  private SchemaUtil(){}

  public static String getSchemaString( final IField schema , final String format ) throws IOException{
    if( "hive".equals( format ) ){
      return HiveSchemaFactory.getHiveSchema( schema ).toString();
    }
    else{
      throw new IOException( String.format( "Unknown format %s." , format ) );
    }
  }

}
