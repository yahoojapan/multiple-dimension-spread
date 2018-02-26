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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.SchemaChangeCallBack;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;

import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionIndex;
import jp.co.yahoo.dataplatform.mds.spread.expression.AllExpressionIndex;
import jp.co.yahoo.dataplatform.mds.spread.expression.IndexFactory;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionNode;
import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;

public class DynamicArrowLoader implements IArrowLoader{

  private final MapVector rootVector;
  private final MDSReader reader;
  private final BufferAllocator allocator;
  private final IRootMemoryAllocator rootMemoryAllocator;

  private IExpressionNode node;

  public DynamicArrowLoader( final IRootMemoryAllocator rootMemoryAllocator , final MDSReader reader , final BufferAllocator allocator ){
    this.reader = reader;
    this.allocator = allocator;
    this.rootMemoryAllocator = rootMemoryAllocator;
    SchemaChangeCallBack callBack = new SchemaChangeCallBack();
    rootVector = new NullableMapVector( "root" , allocator , new FieldType( true , Struct.INSTANCE , null , null ) , callBack );
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
    IMemoryAllocator memoryAllocator = rootMemoryAllocator.create( allocator , rootVector );
    Spread spread = reader.next();
    IExpressionIndex index = new AllExpressionIndex( spread.size() );
    if( node != null ){
      index = IndexFactory.toExpressionIndex( spread , node.exec( spread ) );
      if( index.size() == 0 ){
        memoryAllocator.setValueCount( 0 );
        return rootVector;
      }
    }
    memoryAllocator.setValueCount( index.size() );
    for( IColumn column : spread.getListColumn() ){
      IMemoryAllocator childMemoryAllocator = memoryAllocator.getChild( column.getColumnName() , column.getColumnType() );
      column.setPrimitiveObjectArray( index , 0 , index.size() , childMemoryAllocator );
      childMemoryAllocator.setValueCount( index.size() );
    }
    return rootVector;
  }

  @Override
  public void close() throws IOException{
    rootVector.clear();
    reader.close();
  }

}
