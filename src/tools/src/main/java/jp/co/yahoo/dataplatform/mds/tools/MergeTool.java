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

import java.util.List;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.HelpFormatter;

import jp.co.yahoo.dataplatform.config.Configuration;
import jp.co.yahoo.dataplatform.schema.formatter.IStreamWriter;
import jp.co.yahoo.dataplatform.schema.formatter.IMessageWriter;
import jp.co.yahoo.dataplatform.mds.MDSWriter;
import jp.co.yahoo.dataplatform.mds.MDSReader;

public final class MergeTool{

  private MergeTool(){}

  public static Options createOptions( final String[] args ){

    Option input = OptionBuilder.
      withLongOpt("input").
      withDescription("Input file paths.").
      hasArg().
      isRequired().
      withArgName("input").
      create( 'i' );

    Option ppd = OptionBuilder.
      withLongOpt("projection_pushdown").
      withDescription("Use projection pushdown. Format:\"[ [ \"column1\" , \"[column1-child]\" , \"column1-child-child\" ] [ \"column2\" , ... ] ... ]\"").
      hasArg().
      withArgName("projection_pushdown").
      create( 'p' );

    Option flatten = OptionBuilder.
      withLongOpt("flatten").
      withDescription("Use flatten function.").
      hasArg().
      withArgName("flatten").
      create( 'x' );

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
      .addOption( ppd )
      .addOption( flatten )
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
    String ppd = cl.getOptionValue( "projection_pushdown" , null );
    String flatten = cl.getOptionValue( "flatten" , null );

    OutputStream out = FileUtil.create( output );

    Configuration config = new Configuration();
    if( ppd != null ){
      config.set( "spread.reader.read.column.names" , ppd );
    }

    if( flatten != null ){
      config.set( "spread.reader.flatten.column" , flatten );
    }

    MDSReader reader = new MDSReader();
    MDSWriter writer = new MDSWriter( out , config );

    List<File> mergeList = FileUtil.pathToFileList( input );
    for( File file : mergeList ){
      InputStream in = FileUtil.fopen( file );
      long fileLength = file.length();
      reader.setNewStream( in , fileLength , config );
      while( reader.hasNext() ){
        writer.appendRow( reader.nextRaw() , reader.getCurrentSpreadSize() );
      }
      reader.close();
    }
    writer.close();

    return 0;
  }

  public static void main( final String[] args ) throws IOException{
    System.exit( run( args ) );
  }

}
