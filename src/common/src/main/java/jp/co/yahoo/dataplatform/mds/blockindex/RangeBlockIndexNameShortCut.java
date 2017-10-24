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
package jp.co.yahoo.dataplatform.mds.blockindex;

import jp.co.yahoo.dataplatform.mds.util.Pair;

public final class RangeBlockIndexNameShortCut{

  private static final Pair classNamePair = new Pair();

  static{
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.blockindex.ByteRangeBlockIndex"     , "R0" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.blockindex.ShortRangeBlockIndex"    , "R1" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.blockindex.IntegerRangeBlockIndex"  , "R2" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.blockindex.LongRangeBlockIndex"     , "R3" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.blockindex.FloatRangeBlockIndex"    , "R4" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.blockindex.DoubleRangeBlockIndex"   , "R5" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.blockindex.StringRangeBlockIndex"   , "R6" );

    classNamePair.set( "jp.co.yahoo.dataplatform.mds.blockindex.FullRangeBlockIndex"   , "FR0" );
  }

  private RangeBlockIndexNameShortCut(){}

  public static String getShortCutName( final String className ){
    String shortCutName = classNamePair.getPair2( className );
    if( shortCutName == null ){
      return className;
    }
    return shortCutName;
  }

  public static String getClassName( final String shortCutName ){
    String className = classNamePair.getPair1( shortCutName );
    if( className == null ){
      return shortCutName;
    }
    return className;
  }

}
