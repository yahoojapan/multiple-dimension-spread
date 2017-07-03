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

import java.util.Map;
import java.util.HashMap;

public class MakerCache implements IMakerCache{

  private final Map<String,MakerCache> childBuffer = new HashMap<String,MakerCache>();
  private final Map<String,ICache> cacheContainer = new HashMap<String,ICache>();

  @Override
  public void registerCache( final String cacheName , final ICache cache ){
    cacheContainer.put( cacheName , cache );
  }

  @Override
  public ICache getCache( final String cacheName ){
    return cacheContainer.get( cacheName );
  }

  @Override
  public MakerCache getChild( final String childName ){
    if( ! childBuffer.containsKey( childName ) ){
      childBuffer.put( childName , new MakerCache() );
    }
    return childBuffer.get( childName );
  }

  @Override
  public void clear(){
    for( Map.Entry<String,MakerCache> entry : childBuffer.entrySet() ){
      entry.getValue().clear();
    }
    childBuffer.clear();
    cacheContainer.clear();
  }

}
