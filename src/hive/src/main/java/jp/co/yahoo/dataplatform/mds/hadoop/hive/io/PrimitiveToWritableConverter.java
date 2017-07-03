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
package jp.co.yahoo.dataplatform.mds.hadoop.hive.io;

import java.io.IOException;

import jp.co.yahoo.dataplatform.mds.binary.IBytesLink;
import jp.co.yahoo.dataplatform.mds.spread.column.ICell;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

public final class PrimitiveToWritableConverter{

  private PrimitiveToWritableConverter(){}

  public static Writable convert( final PrimitiveCategory primitiveCategory , final ICell target ) throws IOException{
    Object obj = target.getRow();
    if( ! ( obj instanceof PrimitiveObject ) ){
      return null;
    }
    PrimitiveObject primitiveObject = (PrimitiveObject)obj;

    switch( primitiveCategory ){
      case STRING:
        Text textResult = new Text();
        if( primitiveObject instanceof IBytesLink){
          IBytesLink linkObj = (IBytesLink)primitiveObject;
          textResult.set( linkObj.getLinkBytes() , linkObj.getStart() , linkObj.getLength() );
        }
        else{
          byte[] strBytes = primitiveObject.getBytes();
          textResult.set( strBytes , 0 , strBytes.length );
        }
        return textResult;
      case BINARY:
        BytesWritable bytesResult = new BytesWritable();
        byte[] bytes = primitiveObject.getBytes();
        bytesResult.set( bytes , 0 , bytes.length );
        return bytesResult;
      case BOOLEAN:
        BooleanWritable booleanResult = new BooleanWritable();
        booleanResult.set( primitiveObject.getBoolean() );
        return booleanResult;
      case BYTE:
        ByteWritable byteResult = new ByteWritable();
        try{
          byteResult.set( primitiveObject.getByte() );
        }catch( NumberFormatException | NullPointerException e ){
          return null;
        }
        return byteResult;
      case SHORT:
        ShortWritable shortResult = new ShortWritable();
        try{
          shortResult.set( primitiveObject.getShort() );
        }catch( NumberFormatException | NullPointerException e ){
          return null;
        }
        return shortResult;
      case INT:
        IntWritable intResult = new IntWritable();
        try{
          intResult.set( primitiveObject.getInt() );
        }catch( NumberFormatException | NullPointerException e ){
          return null;
        }
        return intResult;
      case LONG:
        LongWritable longResult = new LongWritable();
        try{
          longResult.set( primitiveObject.getLong() );
        }catch( NumberFormatException | NullPointerException e ){
          return null;
        }
        return longResult;
      case FLOAT:
        FloatWritable floatResult = new FloatWritable();
        try{
          floatResult.set( primitiveObject.getFloat() );
        }catch( NumberFormatException | NullPointerException e ){
          return null;
        }
        return floatResult;
      case DOUBLE:
        DoubleWritable doubleResult = new DoubleWritable();
        try{
          doubleResult.set( primitiveObject.getDouble() );
        }catch( NumberFormatException | NullPointerException e ){
          return null;
        }
        return doubleResult;
      case DATE:
      case DECIMAL:
      case TIMESTAMP:
      case VOID:
      default:
        throw new UnsupportedOperationException( "Unknown category " + primitiveCategory );
    }
  }

}
