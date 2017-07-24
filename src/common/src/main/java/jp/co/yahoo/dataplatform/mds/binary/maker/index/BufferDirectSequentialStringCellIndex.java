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
package jp.co.yahoo.dataplatform.mds.binary.maker.index;

import java.io.IOException;
import java.nio.IntBuffer;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import jp.co.yahoo.dataplatform.mds.binary.maker.IDicManager;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.IFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.IStringFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.IStringCompareFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.IStringComparator;
import jp.co.yahoo.dataplatform.mds.spread.column.index.ICellIndex;

public class BufferDirectSequentialStringCellIndex implements ICellIndex{

  private final IDicManager dicManager;
  private final IntBuffer dicIndexIntBuffer;

  public BufferDirectSequentialStringCellIndex( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer ){
    this.dicManager = dicManager;
    this.dicIndexIntBuffer = dicIndexIntBuffer;
  }

  @Override
  public List<Integer> filter( final IFilter filter ) throws IOException{
    switch( filter.getFilterType() ){
      case STRING:
        IStringFilter stringFilter = (IStringFilter)filter;
        String targetStr = stringFilter.getSearchString(); 
        switch( stringFilter.getStringFilterType() ){
          case PERFECT:
            return toColumnList( perfectMatch( targetStr ) );
          case PARTIAL:
            return toColumnList( partialMatch( targetStr ) );
          case FORWARD:
            return toColumnList( forwardMatch( targetStr ) );
          case BACKWARD:
            return toColumnList( backwardMatch( targetStr ) );
          case REGEXP:
            return toColumnList( regexpMatch( targetStr ) );
          default:
            return null;
        }
      case STRING_COMPARE:
        IStringCompareFilter stringCompareFilter = (IStringCompareFilter)filter;
        IStringComparator comparator = stringCompareFilter.getStringComparator();
          return toColumnList( compareString( comparator ) );
      default:
        return null;
    }
  }

  private List<Integer> toColumnList( final Set<Integer> targetDicSet ){
    if( targetDicSet.isEmpty() ){
      return new ArrayList<Integer>();
    }
    int length = dicIndexIntBuffer.capacity();
    List<Integer> result = new ArrayList<Integer>( length );
    for( int i = 0 ; i < length ; i++ ){
      Integer dicIndex = Integer.valueOf( dicIndexIntBuffer.get(i) );
      if( targetDicSet.contains( dicIndex ) ){
        result.add( Integer.valueOf( i ) );
      }
    }
    return result;
  }

  private Set<Integer> compareString( final IStringComparator comparator ) throws IOException{
    Set<Integer> matchDicList = new HashSet<Integer>();
    for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
      if( ! comparator.isFilterString( dicManager.get( i ).getString() ) ){
        matchDicList.add( Integer.valueOf( i ) );
      }
    }

    return matchDicList;
  }

  private Set<Integer> perfectMatch( final String targetStr ) throws IOException{
    Set<Integer> matchDicList = new HashSet<Integer>();
    for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
      if( targetStr.equals( dicManager.get( i ).getString() ) ){
        matchDicList.add( Integer.valueOf( i ) );
      }
    }

    return matchDicList;
  }

  private Set<Integer> partialMatch( final String targetStr ) throws IOException{
    Set<Integer> matchDicList = new HashSet<Integer>();
    for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
      if( ( -1 < dicManager.get( i ).getString().indexOf( targetStr) ) ){
        matchDicList.add( Integer.valueOf( i ) );
      }
    }

    return matchDicList;
  }

  private Set<Integer> forwardMatch( final String targetStr ) throws IOException{
    Set<Integer> matchDicList = new HashSet<Integer>();
    for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
      if( dicManager.get( i ).getString().startsWith( targetStr ) ){
        matchDicList.add( Integer.valueOf( i ) );
      }
    }

    return matchDicList;
  }

  private Set<Integer> backwardMatch( final String targetStr ) throws IOException{
    Set<Integer> matchDicList = new HashSet<Integer>();
    for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
      if( dicManager.get( i ).getString().endsWith( targetStr ) ){
        matchDicList.add( Integer.valueOf( i ) );
      }
    }

    return matchDicList;
  }

  private Set<Integer> regexpMatch( final String targetStr ) throws IOException{
    Set<Integer> matchDicList = new HashSet<Integer>();
    for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
      if( dicManager.get( i ).getString().matches( targetStr ) ){
        matchDicList.add( Integer.valueOf( i ) );
      }
    }

    return matchDicList;
  }

}
