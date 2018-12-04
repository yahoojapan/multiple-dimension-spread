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

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;

import java.util.List;
import java.util.ArrayList;

public final class FileUtil{

  public static InputStream fopen( final String input ) throws IOException{
    if( input == null ){
      throw new IOException( "Input path is null." );
    }

    if( input.isEmpty() ){
      throw new IOException( "Input path is empty." );
    }

    if( "-".equals( input ) ){
      return new BufferedInputStream( System.in );
    }

    return fopen( new File( input ) );
  }

  public static InputStream fopen( final File file ) throws IOException{
    if( ! file.exists() ){
      throw new IOException( String.format( "Input path does not find : %s" , file.toString() ) );
    }

    return new FileInputStream( file );
  }

  public static OutputStream create( final String output ) throws IOException{
    if( output == null ){
      throw new IOException( "Output path is null." );
    }

    if( output.isEmpty() ){
      throw new IOException( "Output path is empty." );
    }

    if( "-".equals( output ) ){
      return new BufferedOutputStream( System.out );
    }

    File file = new File( output );
    if( file.exists() ){
      throw new IOException( String.format( "Output path is already exists : %s" , output ) );
    }

    return new FileOutputStream( file );
  }

  public static List<File> pathToFileList( final String target ){
    return pathToFileList( new File( target ) );
  }

  public static List<File> pathToFileList( final File path ){
    List<File> result = new ArrayList<File>();
    if( path.isHidden() ){
      return result;
    }

    if( path.isDirectory() ){
      for( File childFile : path.listFiles() ){
        result.addAll( pathToFileList( childFile ) );
      }
    }
    else if( path.isFile() ){
      result.add( path );
    }
    return result;
  }

}
