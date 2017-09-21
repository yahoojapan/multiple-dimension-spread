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
package jp.co.yahoo.dataplatform.mds.binary;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import java.util.List;

import jp.co.yahoo.dataplatform.mds.stats.SummaryStats;
import jp.co.yahoo.dataplatform.mds.stats.ColumnStats;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnTypeFactory;
import jp.co.yahoo.dataplatform.mds.compressor.CompressorNameShortCut;

public class ColumnBinary{

  public final String makerClassName;

  public final String compressorClassName;

  public final String columnName;
  public final ColumnType columnType;
  public final int rowCount;
  public final int rawDataSize;
  public final int logicalDataSize;
  public final int cardinality;

  public int binaryStart;
  public int binaryLength;
  public byte[] binary;

  public List<ColumnBinary> columnBinaryList;

  public ColumnBinary( final String makerClassName , final String compressorClassName , final String columnName , final ColumnType columnType , final int rowCount , final int rawDataSize , final int logicalDataSize , int cardinality , final byte[] binary , final int binaryStart , final int binaryLength , final List<ColumnBinary> columnBinaryList ){
    this.makerClassName = makerClassName;
    this.compressorClassName = compressorClassName;
    this.columnName = columnName;
    this.columnType = columnType;
    this.rowCount = rowCount;
    this.rawDataSize = rawDataSize;
    this.logicalDataSize = logicalDataSize;
    this.cardinality = cardinality;
    this.binaryStart = binaryStart;
    this.binaryLength = binaryLength;
    this.binary = binary;
    this.columnBinaryList = columnBinaryList;
  }

  public int size() throws IOException{
    int length = 
      ( makerClassName.length() * Character.BYTES )
      + Integer.BYTES
      + ( compressorClassName.length() * Character.BYTES )
      + Integer.BYTES
      + ( columnName.length() * Character.BYTES )
      + Integer.BYTES
      + Byte.BYTES
      + Integer.BYTES
      + Integer.BYTES
      + Integer.BYTES
      + Integer.BYTES
      + Integer.BYTES
      + Integer.BYTES
      + binaryLength;

    if( columnBinaryList != null ){
      for( ColumnBinary child : columnBinaryList ){
        length += child.size();
      }
    }
    return length;
  }

  public int getMetaSize() throws IOException{
    return 
      ( ColumnBinaryMakerNameShortCut.getShortCutName( makerClassName ).length() * Character.BYTES )
      + Integer.BYTES
      + ( CompressorNameShortCut.getShortCutName( compressorClassName ).length() * Character.BYTES )
      + Integer.BYTES
      + ( columnName.length() * Character.BYTES )
      + Integer.BYTES
      + Byte.BYTES
      + Integer.BYTES
      + Integer.BYTES
      + Integer.BYTES
      + Integer.BYTES
      + Integer.BYTES
      + Integer.BYTES;
  }

  public static ColumnBinary newInstanceFromMetaBinary( final byte[] metaBinary , final int start , final int length , final byte[] dataBuffer , final List<ColumnBinary> childList ) throws IOException{
    int offset = start;
    ByteBuffer wrapBuffer = ByteBuffer.wrap( metaBinary , start , length );
    CharBuffer viewCharBuffer = wrapBuffer.asCharBuffer();

    int classNameLength = wrapBuffer.getInt( offset );
    offset += Integer.BYTES;
    viewCharBuffer.position( ( offset - start ) / Character.BYTES );
    char[] classNameChars = new char[ classNameLength / Character.BYTES ];
    viewCharBuffer.get( classNameChars );
    String metaClassName = String.valueOf( classNameChars );
    offset += classNameLength;

    int compressorClassNameLength = wrapBuffer.getInt( offset );
    offset += Integer.BYTES;
    viewCharBuffer.position( ( offset - start ) / Character.BYTES );
    char[] compressorClassNameChars = new char[ compressorClassNameLength / Character.BYTES ];
    viewCharBuffer.get( compressorClassNameChars );
    String metaCompressorClassName = String.valueOf( compressorClassNameChars );
    offset += compressorClassNameLength;

    int columnNameLength = wrapBuffer.getInt( offset );
    offset += Integer.BYTES;
    viewCharBuffer.position( ( offset - start ) / Character.BYTES );
    char[] columnNameChars = new char[ columnNameLength / Character.BYTES ];
    viewCharBuffer.get( columnNameChars );
    String metaColumnName = String.valueOf( columnNameChars );
    offset += columnNameLength;

    byte columnTypeByte = wrapBuffer.get( offset );
    ColumnType metaColumnType = ColumnTypeFactory.getColumnTypeFromByte( columnTypeByte );
    offset += Byte.BYTES;

    int metaRowCount = wrapBuffer.getInt( offset );
    offset += Integer.BYTES;

    int metaRowData = wrapBuffer.getInt( offset );
    offset += Integer.BYTES;

    int metaLogicalData = wrapBuffer.getInt( offset );
    offset += Integer.BYTES;

    int metaCardinality = wrapBuffer.getInt( offset );
    offset += Integer.BYTES;
    
    int metaBinaryStart = wrapBuffer.getInt( offset );
    offset += Integer.BYTES;

    int metaBinaryLength = wrapBuffer.getInt( offset );
    offset += Integer.BYTES;

    return new ColumnBinary( ColumnBinaryMakerNameShortCut.getClassName( metaClassName ) , CompressorNameShortCut.getClassName( metaCompressorClassName ) , metaColumnName , metaColumnType , metaRowCount , metaRowData , metaLogicalData , metaCardinality , dataBuffer , metaBinaryStart , metaBinaryLength , childList );
  }

  public byte[] toMetaBinary() throws IOException{
    String shortCutClassName = ColumnBinaryMakerNameShortCut.getShortCutName( makerClassName );
    String shortCutCompressorClassName = CompressorNameShortCut.getShortCutName( compressorClassName );
    int classNameLength = shortCutClassName.length() * 2;
    int compressorClassNameLength = shortCutCompressorClassName.length() * 2;
    int columnNameLength = columnName.length() * 2;
    byte columnTypeByte = ColumnTypeFactory.getColumnTypeByte( columnType );

    byte[] result = new byte[getMetaSize()];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    CharBuffer viewCharBuffer = wrapBuffer.asCharBuffer();
    int offset = 0;

    wrapBuffer.putInt( offset , classNameLength );
    offset += Integer.BYTES;
    viewCharBuffer.position( offset / Character.BYTES );
    viewCharBuffer.put( shortCutClassName.toCharArray() );
    offset += classNameLength;

    wrapBuffer.putInt( offset , compressorClassNameLength );
    offset += Integer.BYTES;
    viewCharBuffer.position( offset / Character.BYTES );
    viewCharBuffer.put( shortCutCompressorClassName.toCharArray() );
    offset += compressorClassNameLength;

    wrapBuffer.putInt( offset , columnNameLength );
    offset += Integer.BYTES;
    viewCharBuffer.position( offset / Character.BYTES );
    viewCharBuffer.put( columnName.toCharArray() );
    offset += columnNameLength;

    wrapBuffer.put( offset , columnTypeByte );
    offset += Byte.BYTES;

    wrapBuffer.putInt( offset , rowCount );
    offset += Integer.BYTES;

    wrapBuffer.putInt( offset , rawDataSize );
    offset += Integer.BYTES;

    wrapBuffer.putInt( offset , logicalDataSize );
    offset += Integer.BYTES;

    wrapBuffer.putInt( offset , cardinality );
    offset += Integer.BYTES;

    wrapBuffer.putInt( offset , binaryStart );
    offset += Integer.BYTES;

    wrapBuffer.putInt( offset , binaryLength );
    offset += Integer.BYTES;

    return result;
  }

  public SummaryStats toSummaryStats(){
    SummaryStats stats = new SummaryStats( rowCount , rawDataSize , binaryLength , logicalDataSize , cardinality );
    if( columnBinaryList != null ){
      for( ColumnBinary columnBinary : columnBinaryList ){
        stats.merge( columnBinary.toSummaryStats() );
      }
    }
    return stats;
  }

  public ColumnStats toColumnStats(){
    ColumnStats columnStats = new ColumnStats( columnName );
    if( columnType == ColumnType.UNION ){
      for( ColumnBinary columnBinary : columnBinaryList ){
        columnStats.addSummaryStats( columnBinary.columnType , columnBinary.toSummaryStats() );
      }
    }
    else{
      SummaryStats stats = new SummaryStats( rowCount , rawDataSize , binaryLength , logicalDataSize , cardinality );
      columnStats.addSummaryStats( columnType , stats );
      for( ColumnBinary columnBinary : columnBinaryList ){
        columnStats.addChild( columnBinary.columnName , columnBinary.toColumnStats() );
      }
    }

    return columnStats;
  }

}
