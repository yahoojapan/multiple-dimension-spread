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
import jp.co.yahoo.dataplatform.schema.formatter.IStreamWriter;
import jp.co.yahoo.dataplatform.schema.formatter.IMessageWriter;
import jp.co.yahoo.dataplatform.mds.schema.parser.MDSSchemaReader;

public final class ReaderTool{

  private ReaderTool(){}

  public static Options createOptions( final String[] args ){
    Option format = OptionBuilder.
      withLongOpt("format").
      withDescription("Output data format. [json|].").
      hasArg().
      withArgName("format").
      create( 'f' );

    Option input = OptionBuilder.
      withLongOpt("input").
      withDescription("Input file path.  \"-\" standard input").
      hasArg().
      isRequired().
      withArgName("input").
      create( 'i' );

    Option schema = OptionBuilder.
      withLongOpt("schema").
      withDescription("If need a schema with input data format please enter it.").
      hasArg().
      withArgName("schema").
      create( 's' );

    Option ppd = OptionBuilder.
      withLongOpt("projection_pushdown").
      withDescription("Use projection pushdown. Format:\"[ [ \"column1\" , \"[column1-child]\" , \"column1-child-child\" ] [ \"column2\" , ... ] ... ]\"").
      hasArg().
      withArgName("projection_pushdown").
      create( 'p' );

    Option expand = OptionBuilder.
      withLongOpt("expand").
      withDescription("Use expand function.").
      hasArg().
      withArgName("expand").
      create( 'e' );

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
      .addOption( format )
      .addOption( input )
      .addOption( schema )
      .addOption( ppd )
      .addOption( expand )
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
    String format = cl.getOptionValue( "format" , null );
    if( format == null || format.isEmpty() ){
      format = "json";
    }
    String schema = cl.getOptionValue( "schema" , null );
    String output = cl.getOptionValue( "output" , null );
    String ppd = cl.getOptionValue( "projection_pushdown" , null );
    String expand = cl.getOptionValue( "expand" , null );
    String flatten = cl.getOptionValue( "flatten" , null );

    OutputStream out = FileUtil.create( output );
    IStreamWriter writer = StreamWriterFactory.create( out , format , schema );

    Configuration config = new Configuration();
    if( ppd != null ){
      config.set( "spread.reader.read.column.names" , ppd );
    }

    if( expand != null ){
      config.set( "spread.reader.expand.column" , expand );
    }

    if( flatten != null ){
      config.set( "spread.reader.flatten.column" , flatten );
    }

    InputStream in = FileUtil.fopen( input );
    MDSSchemaReader reader = new MDSSchemaReader();

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
      reader.setNewStream( in , totalLength , config );
    }
    else{
      File file = new File( input );
      long fileLength = file.length();
      reader.setNewStream( in , fileLength , config );
    }

    while( reader.hasNext() ){
      writer.write( reader.next() );
    }
    writer.close();
    reader.close();

    return 0;
  }

  public static void main( final String[] args ) throws IOException{
    System.exit( run( args ) );
  }

}
