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
package jp.co.yahoo.dataplatform.mds.hadoop.hive.io.vector;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.binary.IBytesLink;
import jp.co.yahoo.dataplatform.mds.spread.column.IColumn;
import jp.co.yahoo.dataplatform.mds.spread.expression.IExpressionIndex;

public class BytesColumnVectorAssignor implements IColumnVectorAssignor{

  private IColumn column;

  @Override
  public void setColumn( final int spreadSize , final IColumn column ) throws IOException{
    this.column = column;
  }

  @Override
  public void setColumnVector( final ColumnVector vector , final IExpressionIndex indexList , final int start , final int length ) throws IOException{
    BytesColumnVector columnVector = (BytesColumnVector)vector;

    PrimitiveObject[] primitiveObjectArray = column.getPrimitiveObjectArray( indexList , start , length );
    for( int i = 0 ; i < length ; i++ ){
      if( primitiveObjectArray[i] == null ){
        VectorizedBatchUtil.setNullColIsNullValue( columnVector , i );
      }
      else{
        if( primitiveObjectArray[i] instanceof IBytesLink ){
          IBytesLink linkObj = (IBytesLink)primitiveObjectArray[i];
          columnVector.vector[i] = linkObj.getLinkBytes();
          columnVector.start[i] = linkObj.getStart();
          columnVector.length[i] = linkObj.getLength();
        }
        else{
          byte[] strBytes = primitiveObjectArray[i].getBytes();
          if( strBytes == null ){
            VectorizedBatchUtil.setNullColIsNullValue( columnVector , i );
          }
          else{
            columnVector.vector[i] = strBytes;
            columnVector.start[i] = 0;
            columnVector.length[i] = strBytes.length;
          }
        }
      }
    }
  }

}
