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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import jp.co.yahoo.dataplatform.schema.parser.IParser;
import jp.co.yahoo.dataplatform.schema.parser.IStreamReader;
import jp.co.yahoo.dataplatform.schema.parser.JacksonMessageReader;

public class LineJsonStreamReader implements IStreamReader{

  private final JacksonMessageReader jsonReader = new JacksonMessageReader();
  private BufferedReader reader;
  private String line;

  public LineJsonStreamReader( final InputStream in ) throws IOException{
    reader = new BufferedReader( new InputStreamReader( in ) );
    line = reader.readLine();
  }

  @Override
  public boolean hasNext() throws IOException{
    return line != null;
  }

  @Override
  public IParser next() throws IOException{
    IParser result = jsonReader.create( line );
    line = reader.readLine();
    return result;
  }

  @Override
  public void close() throws IOException{
    reader.close();
  }

}
