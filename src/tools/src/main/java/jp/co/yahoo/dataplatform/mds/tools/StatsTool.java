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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.HelpFormatter;

import jp.co.yahoo.dataplatform.config.Configuration;

import jp.co.yahoo.dataplatform.schema.design.StructContainerField;

import jp.co.yahoo.dataplatform.mds.MDSStatsReader;
import jp.co.yahoo.dataplatform.mds.stats.SpreadSummaryStats;

public final class StatsTool{

  private static final byte[] NEW_LINE = new byte[]{ '\n' };

  private StatsTool(){}

  public static Options createOptions( final String[] args ){
    Option input = OptionBuilder.
      withLongOpt("input").
      withDescription("Input file path.  \"-\" standard input").
      hasArg().
      isRequired().
      withArgName("input").
      create( 'i' );

    Option output = OptionBuilder.
      withLongOpt("output").
      withDescription("output file path. \"-\" standard output").
      hasArg().
      isRequired().
      withArgName("output").
      create( 'o' );

    Option help = OptionBuilder.
      withLongOpt("help").
      withDescription("help").
      withArgName("help").
      create( 'h' );

    Options  options = new Options();

    return options
      .addOption( input )
      .addOption( output )
      .addOption( help );
  }

  public static void printHelp( final String[] args ){
    HelpFormatter hf = new HelpFormatter();
    hf.printHelp( "[options]" , createOptions( args ) );
  }

  public static int run( final String[] args ) throws IOException{
    CommandLine cl;
    try{
      CommandLineParser clParser = new GnuParser();
      cl = clParser.parse( createOptions( args ) , args );
    }catch( ParseException e ){
      printHelp( args );
      throw new IOException( e );
    }

    if( cl.hasOption( "help" ) ){
      printHelp( args );
      return 0;
    }

    String input = cl.getOptionValue( "input" , null );
    String output = cl.getOptionValue( "output" , null );

    OutputStream out = FileUtil.create( output );

    Configuration config = new Configuration();

    InputStream in = FileUtil.fopen( input );
    MDSStatsReader reader = new MDSStatsReader();

    if( "-".equals( input ) ){
      ByteArrayOutputStream bOut = new ByteArrayOutputStream();
      byte[] buffer = new byte[1024*10];
      int readLength = in.read( buffer );
      long totalLength = readLength;
      while( 0 <= readLength ){
        bOut.write( buffer );
        readLength = in.read( buffer );
        totalLength += readLength;
      }
      in = new ByteArrayInputStream( bOut.toByteArray() );
      reader.readStream( in , totalLength , config );
    }
    else{
      File file = new File( input );
      long fileLength = file.length();
      reader.readStream( in , fileLength , config );
    }

    SpreadSummaryStats rootStats = reader.getTotalSummaryStats();

    out.write( rootStats.toString().getBytes() );
    out.write( NEW_LINE );
    out.close();

    return 0;
  }

  public static void main( final String[] args ) throws IOException{
    System.exit( run( args ) );
  }

}
