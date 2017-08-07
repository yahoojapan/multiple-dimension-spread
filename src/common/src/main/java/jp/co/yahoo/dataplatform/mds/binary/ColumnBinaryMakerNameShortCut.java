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
package jp.co.yahoo.dataplatform.mds.binary;

import jp.co.yahoo.dataplatform.mds.util.Pair;

public final class ColumnBinaryMakerNameShortCut{

  private static final Pair classNamePair = new Pair();

  static{
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpArrayColumnBinaryMaker"   , "D0" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpBooleanColumnBinaryMaker" , "D1" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpByteColumnBinaryMaker"    , "D2" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpBytesColumnBinaryMaker"   , "D3" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpDoubleColumnBinaryMaker"  , "D4" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpFloatColumnBinaryMaker"   , "D5" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpIntegerColumnBinaryMaker" , "D6" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpLongColumnBinaryMaker"    , "D7" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpShortColumnBinaryMaker"   , "D8" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpSpreadColumnBinaryMaker"  , "D9" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpStringColumnBinaryMaker"  , "D10" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpUnionColumnBinaryMaker"   , "D11" );

    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.UniqByteColumnBinaryMaker"              , "U0" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.UniqDoubleColumnBinaryMaker"            , "U1" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.UniqFloatColumnBinaryMaker"             , "U2" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.UniqIntegerColumnBinaryMaker"           , "U3" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.UniqLongColumnBinaryMaker"              , "U4" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.UniqShortColumnBinaryMaker"             , "U5" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.UniqStringColumnBinaryMaker"            , "U6" );
    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.UniqStringToUTF8BytesColumnBinaryMaker" , "U7" );

    classNamePair.set( "jp.co.yahoo.dataplatform.mds.binary.maker.RangeIndexStringToUTF8BytesColumnBinaryMaker" , "R0" );
  }

  private ColumnBinaryMakerNameShortCut(){}

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
