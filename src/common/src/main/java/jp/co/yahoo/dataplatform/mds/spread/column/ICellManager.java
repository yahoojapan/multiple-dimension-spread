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
package jp.co.yahoo.dataplatform.mds.spread.column;

import java.io.IOException;

import java.util.List;

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.inmemory.IMemoryAllocator;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.IFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.index.ICellIndex;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionIndex;

public interface ICellManager{

  void add( final ICell cell , final int index );

  ICell get( final int index , final ICell defaultCell );

  int getMaxIndex();

  int size();

  void clear();

  void setIndex( final ICellIndex index );

  boolean[] filter( final IFilter filter , final boolean[] filterArray ) throws IOException;

  PrimitiveObject[] getPrimitiveObjectArray( final IExpressionIndex indexList , final int start , final int length );

  void setPrimitiveObjectArray( final IExpressionIndex indexList , final int start , final int length , final IMemoryAllocator allocator );

}
