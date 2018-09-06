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

public final class MDSTool{

  private MDSTool(){}

  public static void printHelp(){
  }

  public static void main( final String[] args ) throws IOException{
    if( args.length == 0 ){
      printHelp();
      System.exit( 255 );
    }

    String command = args[0];
    String[] commandArgs = new String[ args.length - 1 ];
    System.arraycopy( args , 1 , commandArgs , 0 , commandArgs.length );
    if( "create".equals( command ) ){
      WriterTool.main( commandArgs );
    }
    else if( "cat".equals( command ) ){
      ReaderTool.main( commandArgs );
    }
    else if( "schema".equals( command ) ){
      SchemaTool.main( commandArgs );
    }
    else if( "fstats".equals( command ) ){
      StatsTool.main( commandArgs );
    }
    else if( "cstats".equals( command ) ){
      ColumnStatsTool.main( commandArgs );
    }
    else{
      System.err.println( String.format( "Unknown command %s" , command ) );
      printHelp();
      System.exit( 255 );
    }
  }

}
