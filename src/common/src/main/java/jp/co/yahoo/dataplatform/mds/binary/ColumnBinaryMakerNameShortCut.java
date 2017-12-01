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

  private static final Pair CLASS_NAME_PAIR = new Pair();

  static{
    CLASS_NAME_PAIR.set( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpArrayColumnBinaryMaker"   , "D0" );
    CLASS_NAME_PAIR.set( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpBooleanColumnBinaryMaker" , "D1" );
    CLASS_NAME_PAIR.set( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpBytesColumnBinaryMaker"   , "D3" );
    CLASS_NAME_PAIR.set( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpDoubleColumnBinaryMaker"  , "D4" );
    CLASS_NAME_PAIR.set( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpFloatColumnBinaryMaker"   , "D5" );
    CLASS_NAME_PAIR.set( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpSpreadColumnBinaryMaker"  , "D9" );
    CLASS_NAME_PAIR.set( "jp.co.yahoo.dataplatform.mds.binary.maker.DumpUnionColumnBinaryMaker"   , "D11" );

    CLASS_NAME_PAIR.set( "jp.co.yahoo.dataplatform.mds.binary.maker.RangeDumpDoubleColumnBinaryMaker"  , "RD0" );
    CLASS_NAME_PAIR.set( "jp.co.yahoo.dataplatform.mds.binary.maker.RangeDumpFloatColumnBinaryMaker"   , "RD5" );
    // Mistake
    CLASS_NAME_PAIR.set( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeLongColumnBinaryMaker"   , "OD0" );
    CLASS_NAME_PAIR.set( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeLongColumnBinaryMaker"   , "O0" );
    CLASS_NAME_PAIR.set( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeFloatColumnBinaryMaker"   , "O1" );
    CLASS_NAME_PAIR.set( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeDoubleColumnBinaryMaker"   , "O2" );
    CLASS_NAME_PAIR.set( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeStringColumnBinaryMaker"   , "O11" );

    CLASS_NAME_PAIR.set( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeDumpLongColumnBinaryMaker"   , "OD10" );
    CLASS_NAME_PAIR.set( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeDumpStringColumnBinaryMaker"   , "OD11" );

    CLASS_NAME_PAIR.set( "jp.co.yahoo.dataplatform.mds.binary.maker.OptimizeIndexDumpStringColumnBinaryMaker"   , "OI11" );

    CLASS_NAME_PAIR.set( "jp.co.yahoo.dataplatform.mds.binary.maker.ConstantColumnBinaryMaker"   , "C0" );
  }

  private ColumnBinaryMakerNameShortCut(){}

  public static String getShortCutName( final String className ){
    String shortCutName = CLASS_NAME_PAIR.getPair2( className );
    if( shortCutName == null ){
      return className;
    }
    return shortCutName;
  }

  public static String getClassName( final String shortCutName ){
    String className = CLASS_NAME_PAIR.getPair1( shortCutName );
    if( className == null ){
      return shortCutName;
    }
    return className;
  }

}
