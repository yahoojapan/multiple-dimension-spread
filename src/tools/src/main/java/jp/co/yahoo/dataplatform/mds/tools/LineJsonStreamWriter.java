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
package jp.co.yahoo.dataplatform.mds.tools;

import java.io.IOException;
import java.io.OutputStream;

import java.util.List;
import java.util.Map;

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;
import jp.co.yahoo.dataplatform.schema.parser.IParser;
import jp.co.yahoo.dataplatform.schema.formatter.IStreamWriter;
import jp.co.yahoo.dataplatform.schema.formatter.JacksonMessageWriter;

public class LineJsonStreamWriter implements IStreamWriter{

  private final byte[] NEW_LINE = new byte[]{ '\n' };
  private final JacksonMessageWriter jsonWriter = new JacksonMessageWriter();
  private final OutputStream out;

  public LineJsonStreamWriter( final OutputStream out ) throws IOException{
    this.out = out;
  }

  @Override
  public void write( final PrimitiveObject obj ) throws IOException{
    byte[] json = jsonWriter.create( obj );
    write( json );
  }

  @Override
  public void write( final List<Object> array ) throws IOException{
    byte[] json = jsonWriter.create( array );
    write( json );
  }

  @Override
  public void write( final Map<Object,Object> map ) throws IOException{
    byte[] json = jsonWriter.create( map );
    write( json );
  }

  @Override
  public void write( final IParser parser ) throws IOException{
    byte[] json = jsonWriter.create( parser );
    write( json );
  }

  private void write( final byte[] json ) throws IOException{
    out.write( json );
    out.write( NEW_LINE );
  }

  @Override
  public void close() throws IOException{
    out.close();
  }

}
