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
import java.util.ArrayList;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.HelpFormatter;

import jp.co.yahoo.dataplatform.config.Configuration;

import jp.co.yahoo.dataplatform.mds.MDSReader;
import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.stats.SummaryStats;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.FindColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;

public final class StoragePerformanceTool{

  private static final byte[] NEW_LINE = new byte[]{ '\n' };

  private StoragePerformanceTool(){}

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

    Option spreadCount = OptionBuilder.
      withLongOpt("maxSpreadCount").
      withDescription("Max spread count.").
      hasArg().
      withArgName("spread_count").
      create( 'n' );

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
      .addOption( spreadCount )
      .addOption( output )
      .addOption( help );
  }

  public static void printHelp( final String[] args ){
    HelpFormatter hf = new HelpFormatter();
    hf.printHelp( "[options]" , createOptions( args ) );
  }

  public static ColumnMakerPerformanceResult storagePerformanceTest( final String columnPath , final IColumn column , final IColumnBinaryMaker maker , final ICompressor compressor ) throws IOException{
    ColumnBinaryMakerConfig commonConfig = new ColumnBinaryMakerConfig();
    commonConfig.compressorClass = compressor;

    Runtime.getRuntime().gc();
    CpuTimeUtil.CpuTime toBinaryStart = CpuTimeUtil.getCurrentCpuTime( Thread.currentThread().getId() );
    ColumnBinary columnBinary = maker.toBinary( commonConfig , null , column );
    CpuTimeUtil.CpuTime toBinaryEnd = CpuTimeUtil.getCurrentCpuTime( Thread.currentThread().getId() );

    long toBinaryCpuTimeNano = CpuTimeUtil.calcCpuTimeNano( toBinaryStart , toBinaryEnd );

    Runtime.getRuntime().gc();
    CpuTimeUtil.CpuTime toColumnStart = CpuTimeUtil.getCurrentCpuTime( Thread.currentThread().getId() );
    IColumn newColumn = FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
    for( int i = 0 ; i < newColumn.size() ; i++ ){
      newColumn.get(i).getRow();
    }
    CpuTimeUtil.CpuTime toColumnEnd = CpuTimeUtil.getCurrentCpuTime( Thread.currentThread().getId() );
    long toColumnCpuTimeNano = CpuTimeUtil.calcCpuTimeNano( toColumnStart , toColumnEnd );

    SummaryStats stats = columnBinary.toSummaryStats();

    return new ColumnMakerPerformanceResult( columnPath , maker , compressor , stats , toBinaryCpuTimeNano , toColumnCpuTimeNano );
  }

  public static List<ColumnMakerPerformanceResult> runPerformanceTest( final String parent , final IColumn column ) throws IOException{
    String columnPath = String.format( "%s/%s" , parent , column.getColumnName() );
    List<ColumnMakerPerformanceResult> result = new ArrayList<ColumnMakerPerformanceResult>();
    IColumnBinaryMaker[] columnBinaryMakers = StoragePerformanceTarget.getColumnBinaryMaker( column.getColumnType() );
    ICompressor[] compressors = StoragePerformanceTarget.getCompressorClass();
    for( IColumnBinaryMaker maker : columnBinaryMakers ){
      for( ICompressor compressor : compressors ){
        result.add( storagePerformanceTest( columnPath , column , maker , compressor ) );
      }
    }
    for( int i = 0 ; i < column.getColumnSize() ; i++ ){
      IColumn childColumn = column.getColumn( i );
      result.addAll( runPerformanceTest( columnPath , childColumn ) );
    }

    return result;
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
    int spreadCount;
    try{
       spreadCount = Integer.parseInt( cl.getOptionValue( "spread_count" , "1" ) );
    }catch( NumberFormatException e ){
      throw new NumberFormatException( "-n option is number only." );
    }
    if( spreadCount <= 0 ){
      spreadCount = 1;
    }
  

    OutputStream out = FileUtil.create( output );

    Configuration config = new Configuration();
    if( ppd != null ){
      config.set( "spread.reader.read.column.names" , ppd );
    }

    InputStream in = FileUtil.fopen( input );
    MDSReader reader = new MDSReader();

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

    List<ColumnMakerPerformanceResult> performanceResultList = new ArrayList<ColumnMakerPerformanceResult>(); 
    int loopCount = 0;
    while( reader.hasNext() && loopCount < spreadCount ){
      Spread spread = reader.next();
      for( int i = 0 ; i < spread.size() ; i++ ){
        IColumn column = spread.getColumn(i);
        column.get(0).getRow();
        performanceResultList.addAll( runPerformanceTest( "/" , column ) );
      }
      loopCount++;
    }
    reader.close();

    for( ColumnMakerPerformanceResult result : performanceResultList ){
      out.write( result.toString().getBytes() );
      out.write( NEW_LINE );
    }
    out.close();

    return 0;
  }

  public static void main( final String[] args ) throws IOException{
    System.exit( run( args ) );
  }

}
