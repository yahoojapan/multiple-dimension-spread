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

import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;

public class LazyCell implements ICell<PrimitiveObject,PrimitiveObject> {

  private final ColumnType type;
  private final IDicManager dicManager;
  private final int index;

  public LazyCell( final ColumnType type , final IDicManager dicManager , final int index ){
    this.type = type;
    this.dicManager = dicManager;
    this.index = index;
  }

  @Override
  public PrimitiveObject getRow(){
    try{
      return dicManager.get( index );
    }catch( IOException e ){
      throw new UnsupportedOperationException( e );
    }
  }

  @Override
  public void setRow( final PrimitiveObject raw ){
    throw new UnsupportedOperationException( "Unsupported set method." );
  }

  @Override
  public ColumnType getType(){
    return type;
  }

  @Override
  public boolean isPrimitive(){
    return true;
  }

  @Override
  public String toString(){
    try{
      PrimitiveObject obj = dicManager.get( index );
      if( obj == null ){
        return null;
      }
      return obj.getString();
    }catch( IOException e ){
      return null;
    }
  }

}
