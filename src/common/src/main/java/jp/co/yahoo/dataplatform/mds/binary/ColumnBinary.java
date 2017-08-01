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

import static jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength.INT_LENGTH;
import static jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength.CHAR_LENGTH;
import static jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength.BYTE_LENGTH;

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
      ( makerClassName.length() * CHAR_LENGTH )
      + INT_LENGTH
      + ( compressorClassName.length() * CHAR_LENGTH )
      + INT_LENGTH
      + ( columnName.length() * CHAR_LENGTH )
      + INT_LENGTH
      + BYTE_LENGTH
      + INT_LENGTH
      + INT_LENGTH
      + INT_LENGTH
      + INT_LENGTH
      + INT_LENGTH
      + INT_LENGTH
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
      ( ColumnBinaryMakerNameShortCut.getShortCutName( makerClassName ).length() * CHAR_LENGTH )
      + INT_LENGTH
      + ( CompressorNameShortCut.getShortCutName( compressorClassName ).length() * CHAR_LENGTH )
      + INT_LENGTH
      + ( columnName.length() * CHAR_LENGTH )
      + INT_LENGTH
      + BYTE_LENGTH
      + INT_LENGTH
      + INT_LENGTH
      + INT_LENGTH
      + INT_LENGTH
      + INT_LENGTH
      + INT_LENGTH;
  }

  public static ColumnBinary newInstanceFromMetaBinary( final byte[] metaBinary , final int start , final int length , final byte[] dataBuffer , final List<ColumnBinary> childList ) throws IOException{
    int offset = start;
    ByteBuffer wrapBuffer = ByteBuffer.wrap( metaBinary , start , length );
    CharBuffer viewCharBuffer = wrapBuffer.asCharBuffer();

    int classNameLength = wrapBuffer.getInt( offset );
    offset += INT_LENGTH;
    viewCharBuffer.position( ( offset - start ) / CHAR_LENGTH );
    char[] classNameChars = new char[ classNameLength / CHAR_LENGTH ];
    viewCharBuffer.get( classNameChars );
    String metaClassName = String.valueOf( classNameChars );
    offset += classNameLength;

    int compressorClassNameLength = wrapBuffer.getInt( offset );
    offset += INT_LENGTH;
    viewCharBuffer.position( ( offset - start ) / CHAR_LENGTH );
    char[] compressorClassNameChars = new char[ compressorClassNameLength / CHAR_LENGTH ];
    viewCharBuffer.get( compressorClassNameChars );
    String metaCompressorClassName = String.valueOf( compressorClassNameChars );
    offset += compressorClassNameLength;

    int columnNameLength = wrapBuffer.getInt( offset );
    offset += INT_LENGTH;
    viewCharBuffer.position( ( offset - start ) / CHAR_LENGTH );
    char[] columnNameChars = new char[ columnNameLength / CHAR_LENGTH ];
    viewCharBuffer.get( columnNameChars );
    String metaColumnName = String.valueOf( columnNameChars );
    offset += columnNameLength;

    byte columnTypeByte = wrapBuffer.get( offset );
    ColumnType metaColumnType = ColumnTypeFactory.getColumnTypeFromByte( columnTypeByte );
    offset += BYTE_LENGTH;

    int metaRowCount = wrapBuffer.getInt( offset );
    offset += INT_LENGTH;

    int metaRowData = wrapBuffer.getInt( offset );
    offset += INT_LENGTH;

    int metaLogicalData = wrapBuffer.getInt( offset );
    offset += INT_LENGTH;

    int metaCardinality = wrapBuffer.getInt( offset );
    offset += INT_LENGTH;
    
    int metaBinaryStart = wrapBuffer.getInt( offset );
    offset += INT_LENGTH;

    int metaBinaryLength = wrapBuffer.getInt( offset );
    offset += INT_LENGTH;

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
    offset += INT_LENGTH;
    viewCharBuffer.position( offset / CHAR_LENGTH );
    viewCharBuffer.put( shortCutClassName.toCharArray() );
    offset += classNameLength;

    wrapBuffer.putInt( offset , compressorClassNameLength );
    offset += INT_LENGTH;
    viewCharBuffer.position( offset / CHAR_LENGTH );
    viewCharBuffer.put( shortCutCompressorClassName.toCharArray() );
    offset += compressorClassNameLength;

    wrapBuffer.putInt( offset , columnNameLength );
    offset += INT_LENGTH;
    viewCharBuffer.position( offset / CHAR_LENGTH );
    viewCharBuffer.put( columnName.toCharArray() );
    offset += columnNameLength;

    wrapBuffer.put( offset , columnTypeByte );
    offset += BYTE_LENGTH;

    wrapBuffer.putInt( offset , rowCount );
    offset += INT_LENGTH;

    wrapBuffer.putInt( offset , rawDataSize );
    offset += INT_LENGTH;

    wrapBuffer.putInt( offset , logicalDataSize );
    offset += INT_LENGTH;

    wrapBuffer.putInt( offset , cardinality );
    offset += INT_LENGTH;

    wrapBuffer.putInt( offset , binaryStart );
    offset += INT_LENGTH;

    wrapBuffer.putInt( offset , binaryLength );
    offset += INT_LENGTH;

    return result;
  }

  public SummaryStats toSummaryStats(){
    SummaryStats stats = new SummaryStats( rowCount , rawDataSize , binaryLength , logicalDataSize , cardinality );
    if( columnBinaryList != null ){
      for( ColumnBinary columnBinary : columnBinaryList ){
        stats.marge( columnBinary.toSummaryStats() );
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
