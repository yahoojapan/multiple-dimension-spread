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
package jp.co.yahoo.dataplatform.mds.util;

import java.io.InputStream;
import java.io.IOException;

public final class InputStreamUtils{

  private InputStreamUtils(){}

  public static long skip( final InputStream in , final long skip ) throws IOException{
    long skipLength = 0;
    while( skipLength < skip ){
      long skipBytes = in.skip( skip - skipLength );
      if( skipBytes == 0 ){
        break;
      }
      skipLength += skipBytes;
    }
    return skipLength;
  }

  public static int read( final InputStream in , final byte[] buffer , final int start , final int length ) throws IOException{
    int readOffset = 0;
    int offset = start;
    while( readOffset < length ){
      int readLength = in.read( buffer , offset , length - readOffset );
      if( readLength == -1 ){
        break;
      }
      readOffset += readLength;
      offset += readLength;
    }
    return readOffset;
  }


}
