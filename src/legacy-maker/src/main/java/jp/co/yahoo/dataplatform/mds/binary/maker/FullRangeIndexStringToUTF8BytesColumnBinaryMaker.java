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
package jp.co.yahoo.dataplatform.mds.binary.maker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import jp.co.yahoo.dataplatform.schema.objects.StringObj;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.dataplatform.mds.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;
import jp.co.yahoo.dataplatform.mds.blockindex.IBlockIndex;
import jp.co.yahoo.dataplatform.mds.blockindex.FullRangeBlockIndex;
import jp.co.yahoo.dataplatform.mds.blockindex.StringRangeBlockIndex;
import jp.co.yahoo.dataplatform.mds.binary.maker.index.RangeStringIndex;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.mds.spread.column.PrimitiveCell;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class FullRangeIndexStringToUTF8BytesColumnBinaryMaker extends RangeIndexStringToUTF8BytesColumnBinaryMaker{

  @Override
  public void setBlockIndexNode( final BlockIndexNode parentNode , final ColumnBinary columnBinary , final int spreadIndex ) throws IOException{
    ByteBuffer wrapBuffer = ByteBuffer.wrap( columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    CharBuffer viewCharBuffer = wrapBuffer.asCharBuffer();
    int offset = columnBinary.binaryStart;
    int minLength = wrapBuffer.getInt( offset );
    offset += Integer.BYTES;
    int maxLength = wrapBuffer.getInt( offset );
    offset += Integer.BYTES;
    viewCharBuffer.position( ( offset - columnBinary.binaryStart ) / Character.BYTES );
    char[] minCharArray = new char[ minLength / Character.BYTES ];
    viewCharBuffer.get( minCharArray );
    offset += minLength;
    viewCharBuffer.position( ( offset - columnBinary.binaryStart ) / Character.BYTES );
    char[] maxCharArray = new char[ maxLength / Character.BYTES ];
    viewCharBuffer.get( maxCharArray );

    IBlockIndex index = new StringRangeBlockIndex( new String( minCharArray ) , new String( maxCharArray ) );

    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new FullRangeBlockIndex( spreadIndex , index ) );
  }

}
