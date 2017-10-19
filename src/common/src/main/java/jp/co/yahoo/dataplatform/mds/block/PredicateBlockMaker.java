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
package jp.co.yahoo.dataplatform.mds.block;

import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.List;
import java.util.ArrayList;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.dataplatform.config.Configuration;

import jp.co.yahoo.dataplatform.schema.parser.IParser;
import jp.co.yahoo.dataplatform.schema.parser.JacksonMessageReader;

import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.Analyzer;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;
import jp.co.yahoo.dataplatform.mds.compressor.FindCompressor;
import jp.co.yahoo.dataplatform.mds.compressor.GzipCompressor;
import jp.co.yahoo.dataplatform.mds.util.ByteArrayData;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.maker.MakerCache;
import jp.co.yahoo.dataplatform.mds.binary.optimizer.IOptimizerFactory;
import jp.co.yahoo.dataplatform.mds.binary.optimizer.FindOptimizerFactory;
import jp.co.yahoo.dataplatform.mds.binary.optimizer.BinaryMakerOptimizer;

public class PredicateBlockMaker implements IBlockMaker{

  private static final int META_BUFFER_SIZE = 1024 * 1024 * 1;

  private final MakerCache makerCache = new MakerCache();
  private final List<Integer> spreadSizeList = new ArrayList<Integer>();

  private ColumnBinaryMakerCustomConfigNode configNode;
  private ByteArrayData dataBuffer;
  private ByteArrayData metaBuffer;
  private int blockSize;
  private ColumnBinaryTree columnTree;
  private boolean makeCustomConfig;
  private IOptimizerFactory optimizerFactory;

  protected ICompressor compressor = new GzipCompressor();
  protected byte[] headerBytes;
  protected int bufferSize;

  @Override
  public void setup( final int blockSize , final Configuration config ) throws IOException{
    this.blockSize = blockSize;
    spreadSizeList.clear();

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    if( config.containsKey( "spread.column.maker.default.compress.class" ) ){
      defaultConfig.compressorClass = FindCompressor.get( config.get( "spread.column.maker.default.compress.class" ) ); 
    }
 
    if( config.containsKey( "spread.column.maker.setting" ) ){
      JacksonMessageReader jsonReader = new JacksonMessageReader();
      IParser jsonParser = jsonReader.create( config.get( "spread.column.maker.setting" ) );
      configNode = new ColumnBinaryMakerCustomConfigNode( defaultConfig , jsonParser ); 
    }
    else if( config.get( "spread.column.maker.use.auto.optimizer" , "true" ).equals( "true" ) ){
      makeCustomConfig = true;
      optimizerFactory = FindOptimizerFactory.get( config.get( "spread.column.maker.use.auto.optimizer.factory.class" , "jp.co.yahoo.dataplatform.mds.binary.optimizer.DefaultOptimizerFactory" ) , config );
      configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    }
    else{
      configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    }

    dataBuffer = new ByteArrayData( blockSize );
    metaBuffer = new ByteArrayData( blockSize );
    columnTree = new ColumnBinaryTree();

    bufferSize = 0;
    headerBytes = new byte[0];
  }

  @Override
  public void appendHeader( final byte[] headerBytes ){
    this.headerBytes = headerBytes;
  }

  private int getColumnBinarySize( final List<ColumnBinary> binaryList ) throws IOException{
    int length = 0;
    for( ColumnBinary binary : binaryList ){
      length += binary.size();
    }
    return length;
  }

  @Override
  public void append( final int spreadSize , final List<ColumnBinary> binaryList ) throws IOException{
    bufferSize += getColumnBinarySize( binaryList ) + Integer.BYTES * 2;
    spreadSizeList.add( spreadSize );

    columnTree.addChild( binaryList );
    if( blockSize <= size() ){
      throw new IOException( "Buffer overflow." );
    }
  }

  @Override
  public List<ColumnBinary> convertRow( final Spread spread ) throws IOException{
    if( makeCustomConfig ){
      List<IColumnAnalizeResult> analizeResultList = Analyzer.analize( spread );
      BinaryMakerOptimizer optimizer = new BinaryMakerOptimizer( analizeResultList );
      configNode = optimizer.createConfigNode( configNode.getCurrentConfig() , optimizerFactory );
      makeCustomConfig = false;
    }
    List<ColumnBinary> result = new ArrayList<ColumnBinary>();
    for( int i = 0 ; i < spread.getColumnSize() ; i++ ){
      IColumn column = spread.getColumn( i );
      ColumnBinaryMakerConfig commonConfig = configNode.getCurrentConfig();
      ColumnBinaryMakerCustomConfigNode childConfigNode = configNode.getChildConfigNode( column.getColumnName() );
      IColumnBinaryMaker maker = commonConfig.getColumnMaker( column.getColumnType() );
      if( childConfigNode != null ){
        maker = childConfigNode.getCurrentConfig().getColumnMaker( column.getColumnType() );
      }
      result.add( maker.toBinary( commonConfig , childConfigNode , column , makerCache ) );
    }
    return result;
  }

  @Override
  public boolean canAppend( final List<ColumnBinary> binaryList ) throws IOException{
    int length = getColumnBinarySize( binaryList );
    return ( size() + length + Integer.BYTES + Integer.BYTES + ( Integer.BYTES * spreadSizeList.size() + 1 ) ) < blockSize;
  }

  @Override
  public int size(){
    return headerBytes.length + bufferSize + META_BUFFER_SIZE;
  }

  @Override
  public byte[] createFixedBlock() throws IOException{
    return create( blockSize );
  }

  @Override
  public byte[] createVariableBlock() throws IOException{
    return create( -1 );
  }

  @Override
  public byte[] create( final int dataSize ) throws IOException{
    columnTree.create( metaBuffer , dataBuffer );

    byte[] metaBinary = compressor.compress( metaBuffer.getBytes() , 0 , metaBuffer.getLength() );

    byte[] result;
    if( dataSize == -1 ){
      result = new byte[ headerBytes.length + dataBuffer.getLength() + metaBinary.length + Integer.BYTES + Integer.BYTES + ( Integer.BYTES * spreadSizeList.size() ) ];
    }
    else{
      result = new byte[dataSize];
    }

    int offset = 0;
    System.arraycopy( headerBytes , 0 , result , offset , headerBytes.length );
    offset += headerBytes.length;

    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    wrapBuffer.putInt( offset , spreadSizeList.size() );
    offset += Integer.BYTES;
    for( Integer spreadSize : spreadSizeList ){
      wrapBuffer.putInt( offset , spreadSize.intValue() );
      offset += Integer.BYTES;
    }

    wrapBuffer.putInt( offset , metaBinary.length );
    offset += Integer.BYTES;

    System.arraycopy( metaBinary , 0 , result , offset , metaBinary.length );
    offset += metaBinary.length;
    System.arraycopy( dataBuffer.getBytes() , 0 , result , offset , dataBuffer.getLength() );

    spreadSizeList.clear();
    dataBuffer.clear();
    metaBuffer.clear();
    columnTree.clear();
    headerBytes = new byte[0];
    bufferSize = 0;
    return result;
  }

  @Override
  public String getReaderClassName(){
    return PredicateBlockReader.class.getName();
  }

  @Override
  public void close() throws IOException{
    spreadSizeList.clear();
    dataBuffer.clear();
    metaBuffer.clear();
    columnTree.clear();
    bufferSize = 0;
  }

}
