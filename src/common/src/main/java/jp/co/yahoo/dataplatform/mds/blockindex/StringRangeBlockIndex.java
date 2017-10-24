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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import java.util.Set;
import java.util.List;
import java.util.ArrayList;

import jp.co.yahoo.dataplatform.mds.spread.column.filter.IFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.IStringFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.IStringCompareFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.IStringComparator;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.IStringDictionaryFilter;

public class StringRangeBlockIndex implements IBlockIndex{

  private String min;
  private String max;

  public StringRangeBlockIndex(){
    min = null;
    max = null;
  }

  public StringRangeBlockIndex( final String min , final String max ){
    this.min = min;
    this.max = max;
  }

  @Override
  public BlockIndexType getBlockIndexType(){
    return BlockIndexType.RANGE_STRING;
  }

  @Override
  public boolean merge( final IBlockIndex blockIndex ){
    if( ! ( blockIndex instanceof StringRangeBlockIndex ) ){
      return false;
    }
    StringRangeBlockIndex stringRangeBlockIndex = (StringRangeBlockIndex)blockIndex;
    if( 0 < min.compareTo( stringRangeBlockIndex.getMin() ) ){
      min = stringRangeBlockIndex.getMin();
    }
    if( max.compareTo( stringRangeBlockIndex.getMax() ) < 0 ){
      max = stringRangeBlockIndex.getMax();
    }
    return true;
  }

  @Override
  public int getBinarySize(){
    return ( min.length() * 2 ) + ( max.length() * 2 ) + 4 + 4;
  }

  @Override
  public byte[] toBinary(){
    byte[] result = new byte[getBinarySize()];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    wrapBuffer.putInt( min.length() );
    wrapBuffer.putInt( max.length() );
    CharBuffer viewCharBuffer = wrapBuffer.asCharBuffer();
    viewCharBuffer.put( min.toCharArray() );
    viewCharBuffer.put( max.toCharArray() );
    return result;
  }

  @Override
  public void setFromBinary( final byte[] buffer , final int start , final int length ){
    ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer );
    wrapBuffer.position( start );
    int minSize = wrapBuffer.getInt();
    int maxSize = wrapBuffer.getInt();

    char[] minChars = new char[minSize];
    char[] maxChars = new char[maxSize];
    CharBuffer viewCharBuffer = wrapBuffer.asCharBuffer();
    viewCharBuffer.get( minChars );
    viewCharBuffer.get( maxChars );
    min = new String( minChars );
    max = new String( maxChars );
  }

  public List<Integer> getBlockSpreadIndex( final IFilter filter ){
    switch( filter.getFilterType() ){
      case STRING:
        IStringFilter stringFilter = (IStringFilter)filter;
        String targetStr = stringFilter.getSearchString();
        switch( stringFilter.getStringFilterType() ){
          case PERFECT:
            if( min.compareTo( targetStr ) <= 0 && 0 <= max.compareTo( targetStr ) ){
              return null;
            }
            return new ArrayList<Integer>();
          case FORWARD:
            if( targetStr.startsWith( min ) && min.compareTo( targetStr ) <= 0 && 0 <= max.compareTo( targetStr ) ){
              return null;
            }
            return new ArrayList<Integer>();
          default:
            return null;
        }
      case STRING_COMPARE:
        IStringCompareFilter stringCompareFilter = (IStringCompareFilter)filter;
        IStringComparator comparator = stringCompareFilter.getStringComparator();
        if( comparator.isOutOfRange( min , max ) ){
          return new ArrayList<Integer>();
        }
        return null;
      case STRING_DICTIONARY:
        IStringDictionaryFilter stringDictionaryFilter = (IStringDictionaryFilter)filter;
        Set<String> dictionary = stringDictionaryFilter.getDictionary();
        for( String str : dictionary ){
          if( min.compareTo( str ) <= 0 && 0 <= max.compareTo( str ) ){
            return null;
          }
        }
        return new ArrayList<Integer>();
      default:
        return null;
    }
  }

  @Override
  public IBlockIndex getNewInstance(){
    return new StringRangeBlockIndex();
  }

  public String getMin(){
    return min;
  }

  public String getMax(){
    return max;
  }

}
