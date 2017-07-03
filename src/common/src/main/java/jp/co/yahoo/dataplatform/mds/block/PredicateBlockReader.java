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
import java.io.InputStream;
import java.nio.ByteBuffer;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.maker.DefaultPrimitiveObjectConnector;
import jp.co.yahoo.dataplatform.mds.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength;
import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.config.Configuration;

import jp.co.yahoo.dataplatform.mds.spread.flatten.IFlattenFunction;
import jp.co.yahoo.dataplatform.mds.spread.flatten.FlattenFunctionFactory;
import jp.co.yahoo.dataplatform.mds.spread.expand.IExpandFunction;
import jp.co.yahoo.dataplatform.mds.spread.expand.ExpandFunctionFactory;
import jp.co.yahoo.dataplatform.mds.compressor.ICompressor;
import jp.co.yahoo.dataplatform.mds.compressor.GzipCompressor;
import jp.co.yahoo.dataplatform.mds.binary.maker.IPrimitiveObjectConnector;
import jp.co.yahoo.dataplatform.mds.binary.FindColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.util.InputStreamUtils;
import jp.co.yahoo.dataplatform.mds.stats.SummaryStats;

public class PredicateBlockReader implements IBlockReader{

  private final Block block;
  private final ICompressor compressor = new GzipCompressor();
  private final ColumnBinaryTree columnBinaryTree = new ColumnBinaryTree();
  private final List<Integer> spreadSizeList = new ArrayList<Integer>();
  private final SummaryStats readSummaryStats = new SummaryStats();

  private IPrimitiveObjectConnector primitiveObjectConnector;

  private IExpandFunction expandFunction;
  private IFlattenFunction flattenFunction;
  private ColumnNameNode columnFilterNode;
  private byte[] buffer;
  private byte[] metaBinary;

  private byte[] metaBytes;
  private int readCount;

  public PredicateBlockReader(){
    block = new Block();
    buffer = new byte[0];
    metaBytes = new byte[1024*1024*16];
  }

  private String[] mergeLinkColumnName( final String[] original , final String[] merge ){
    if( merge.length == 0 ){
      return original;
    }
    String[] result = new String[ merge.length + original.length - 1];
    System.arraycopy( merge , 0 ,result  , 0 , merge.length );
    System.arraycopy( original , 1 , result , merge.length , original.length - 1 );

    return result;
  }

  @Override
  public void setup( final Configuration config ) throws IOException{
    primitiveObjectConnector = (IPrimitiveObjectConnector)( config.getObject( "binary.primitive.object.connector.class" , DefaultPrimitiveObjectConnector.class.getName() ) );
    expandFunction = ExpandFunctionFactory.get( config );
    flattenFunction = FlattenFunctionFactory.get( config );

    columnFilterNode = new ColumnNameNode( "root" );
    List<String[]> needColumnList = ReadColumnUtil.readColumnSetting( config.get( "spread.reader.read.column.names" ) );
    for( String[] needColumn : needColumnList ){
      String[] flattenColumnNameArray = flattenFunction.getFlattenColumnName( needColumn[0] );
      String[] flattenMergeNeedColumn = mergeLinkColumnName( needColumn , flattenColumnNameArray ); 

      String[] expandColumnNameArray = expandFunction.getExpandLinkColumnName( flattenMergeNeedColumn[0] );
      String[] mergeNeedColumn = mergeLinkColumnName( flattenMergeNeedColumn , expandColumnNameArray ); 

      ColumnNameNode currentColumnNameNode = columnFilterNode;
      for( int i = 0 ; i < mergeNeedColumn.length ; i++ ){
        String columnName = mergeNeedColumn[i];
        ColumnNameNode columnNameNode = currentColumnNameNode.getChild( columnName );
        if( columnNameNode == null ){
          columnNameNode = new ColumnNameNode( columnName );
        }
        if( i == ( mergeNeedColumn.length - 1 ) ){
          columnNameNode.setNeedAllChild( true );
        }
        currentColumnNameNode.addChild( columnNameNode );
        currentColumnNameNode = columnNameNode;
      }
    }
    if( columnFilterNode.isChildEmpty() ){
      columnFilterNode.setNeedAllChild( true );
    }
    else{
      List<String[]> expandNeedColumnList = expandFunction.getExpandColumnName();
      for( String[] needColumn : expandNeedColumnList ){
        ColumnNameNode currentColumnNameNode = columnFilterNode;
        for( int i = 0 ; i < needColumn.length ; i++ ){
          String columnName = needColumn[i];
          ColumnNameNode columnNameNode = currentColumnNameNode.getChild( columnName );
          if( columnNameNode == null ){
            columnNameNode = new ColumnNameNode( columnName );
            currentColumnNameNode.addChild( columnNameNode );
          }
          currentColumnNameNode = columnNameNode;
        }
      }
    }
  }

  @Override
  public void setBlockSize( final int blockSize ){
    if( buffer.length < blockSize ){
      buffer = new byte[blockSize];
    }
  }

  @Override
  public void setStream( final InputStream in , final int blockSize ) throws IOException{
    spreadSizeList.clear();
    columnBinaryTree.clear();
    columnBinaryTree.setColumnFilter( columnFilterNode );
    if( buffer.length < blockSize ){
      buffer = new byte[blockSize];
    }

    byte[] spreadSizeLengthBytes = new byte[PrimitiveByteLength.INT_LENGTH];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( spreadSizeLengthBytes );
    InputStreamUtils.read( in , spreadSizeLengthBytes , 0 , PrimitiveByteLength.INT_LENGTH );
    int spreadSizeLength = wrapBuffer.getInt(0);

    byte[] spreadSizeBytes = new byte[ PrimitiveByteLength.INT_LENGTH * spreadSizeLength ];
    wrapBuffer = ByteBuffer.wrap( spreadSizeBytes );
    InputStreamUtils.read( in , spreadSizeBytes , 0 , PrimitiveByteLength.INT_LENGTH * spreadSizeLength );
    for( int i = 0 ; i < spreadSizeLength ; i++ ){
      spreadSizeList.add( wrapBuffer.getInt() );
    }

    byte[] lengthBytes = new byte[PrimitiveByteLength.INT_LENGTH];
    wrapBuffer = ByteBuffer.wrap( lengthBytes );
    InputStreamUtils.read( in , lengthBytes , 0 , PrimitiveByteLength.INT_LENGTH );

    int metaLength = wrapBuffer.getInt( 0 );
    if( metaBytes.length < metaLength ){
      metaBytes = new byte[metaLength];
    }

    InputStreamUtils.read( in , metaBytes , 0 , metaLength );

    int decompressSize = compressor.getDecompressSize( metaBytes , 0 , metaLength );
    if( metaBinary == null || metaBinary.length < decompressSize ){
      metaBinary = new byte[decompressSize];
    }
    int binaryLength = compressor.decompressAndSet(  metaBytes , 0 , metaLength , metaBinary );
    columnBinaryTree.toColumnBinaryTree( metaBinary , 0 , buffer );

    block.setColumnBinaryTree( columnBinaryTree );

    int dataBufferLength = blockSize - metaLength - PrimitiveByteLength.INT_LENGTH - PrimitiveByteLength.INT_LENGTH - PrimitiveByteLength.INT_LENGTH * spreadSizeLength;
    if( columnFilterNode.isChildEmpty() ){
      InputStreamUtils.read( in , buffer , 0 , dataBufferLength );
    }
    else{
      List<BlockReadOffset> readOffsetList = columnBinaryTree.getBlockReadOffset();
      List<BlockReadOffset> margeList = new ArrayList<BlockReadOffset>();
      int currentStart = 0;
      int totalLength = 0;
      Collections.sort( readOffsetList );
      for( BlockReadOffset blockReadOffset : readOffsetList ){
        if( ( currentStart + totalLength ) == blockReadOffset.start ){
          totalLength += blockReadOffset.length;
        }
        else{
          if( totalLength != 0 ){
            margeList.add( new BlockReadOffset( currentStart , totalLength ) );
          }
          currentStart = blockReadOffset.start;
          totalLength = blockReadOffset.length;
        }
      }
      if( totalLength != 0 ){
        margeList.add( new BlockReadOffset( currentStart , totalLength ) );
      }

      int inOffset = 0;
      for( BlockReadOffset blockReadOffset : margeList ){
        inOffset += InputStreamUtils.skip( in , blockReadOffset.start - inOffset );
        inOffset = blockReadOffset.start;
        inOffset += InputStreamUtils.read( in , buffer , inOffset , blockReadOffset.length );
      }
      if( inOffset < dataBufferLength ){
        inOffset += InputStreamUtils.skip( in , dataBufferLength - inOffset );
      }
    }

    readCount = 0;
  }

  @Override
  public boolean hasNext() throws IOException{
    return readCount < block.size();
  }

  @Override
  public Spread next() throws IOException{
    Spread spread = new Spread();
    int spreadSize = spreadSizeList.get( readCount ).intValue();
    for( ColumnBinary columnBinary : block.get( readCount ) ){
      if( columnBinary != null ){
        IColumnBinaryMaker maker = FindColumnBinaryMaker.get( columnBinary.makerClassName );
        spread.addColumn( maker.toColumn( columnBinary , primitiveObjectConnector ) );
        readSummaryStats.marge( columnBinary.toSummaryStats() );
      }
    }
    spread.setRowCount( spreadSize );

    readCount++;
    Spread expandSpread = expandFunction.expand( spread );
    return flattenFunction.flatten( expandSpread );
  }

  @Override
  public List<ColumnBinary> nextRaw() throws IOException{
    List<ColumnBinary> columnBinaryList = block.get( readCount );
    readCount++;
    return columnBinaryList;
  }

  @Override
  public int getBlockReadCount(){
    return readCount;
  }

  @Override
  public int getBlockCount(){
    return block.size();
  }

  @Override
  public SummaryStats getReadStats(){
    return readSummaryStats;
  }

  @Override
  public Integer getCurrentSpreadSize(){
    return spreadSizeList.get( readCount - 1 );
  }

  @Override
  public void close() throws IOException{
  }

}
