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
package jp.co.yahoo.dataplatform.mds;

import java.io.IOException;

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.SchemaChangeCallBack;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;

import jp.co.yahoo.dataplatform.mds.binary.ColumnBinary;
import jp.co.yahoo.dataplatform.mds.binary.FindColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionNode;
import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;

public class DirectArrowLoader implements IArrowLoader{

  private final StructVector rootVector;
  private final MDSReader reader;
  private final BufferAllocator allocator;
  private final IRootMemoryAllocator rootMemoryAllocator;

  private IExpressionNode node;

  public DirectArrowLoader( final IRootMemoryAllocator rootMemoryAllocator , final MDSReader reader , final BufferAllocator allocator ){
    this.reader = reader;
    this.allocator = allocator;
    this.rootMemoryAllocator = rootMemoryAllocator;
    SchemaChangeCallBack callBack = new SchemaChangeCallBack();
    rootVector = new StructVector( "root" , allocator , new FieldType( true , Struct.INSTANCE , null , null ) , callBack );
  }

  @Override
  public void setNode( final IExpressionNode node ){
    this.node = node;
  }

  @Override
  public boolean hasNext() throws IOException{
    return reader.hasNext();
  }

  @Override
  public ValueVector next() throws IOException{
    rootVector.clear();
    List<ColumnBinary> columnBinaryList = reader.nextRaw();
    int rowCount = reader.getCurrentSpreadSize();
    IMemoryAllocator memoryAllocator = rootMemoryAllocator.create( allocator , rootVector , rowCount );

    if( node != null ){
      BlockIndexNode blockIndexNode = new BlockIndexNode();
      for( ColumnBinary columnBinary : columnBinaryList ){
        IColumnBinaryMaker maker = FindColumnBinaryMaker.get( columnBinary.makerClassName );
        maker.setBlockIndexNode( blockIndexNode , columnBinary , 0 );
      }
      List<Integer> blockIndexList = node.getBlockSpreadIndex( blockIndexNode );
      if( blockIndexList != null && blockIndexList.isEmpty() ){
        memoryAllocator.setValueCount( 0 );
        return rootVector;
      }
    }

    int spreadSize = reader.getCurrentSpreadSize();
    memoryAllocator.setValueCount( spreadSize );
    for( ColumnBinary columnBinary : columnBinaryList ){
      IColumnBinaryMaker maker = FindColumnBinaryMaker.get( columnBinary.makerClassName );
      IMemoryAllocator childMemoryAllocator = memoryAllocator.getChild( columnBinary.columnName , columnBinary.columnType );
      maker.loadInMemoryStorage( columnBinary , childMemoryAllocator );
      childMemoryAllocator.setValueCount( spreadSize );
    }
    return rootVector;
  }

  @Override
  public void close() throws IOException{
    rootVector.clear();
    reader.close();
  }

}
