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

import jp.co.yahoo.dataplatform.schema.objects.PrimitiveObject;

import jp.co.yahoo.dataplatform.mds.binary.maker.IDicManager;
import jp.co.yahoo.dataplatform.mds.util.NumberUtils;
import jp.co.yahoo.dataplatform.mds.spread.column.ColumnType;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.IFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.NumberFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.NumberRangeFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.index.ICellIndex;

public class BufferDirectSequentialNumberCellIndex implements ICellIndex{

  private final IComparator comparator;
  private final IDicManager dicManager;
  private final IntBuffer dicIndexIntBuffer;

  public BufferDirectSequentialNumberCellIndex( final ColumnType columnType , final IDicManager dicManager , final IntBuffer dicIndexIntBuffer ) throws IOException{
    this.dicManager = dicManager;
    this.dicIndexIntBuffer = dicIndexIntBuffer;
    switch( columnType ){
      case BYTE:
        comparator = new ByteComparator();
        break;
      case SHORT:
        comparator = new ShortComparator();
        break;
      case INTEGER:
        comparator = new IntegerComparator();
        break;
      case LONG:
        comparator = new LongComparator();
        break;
      case FLOAT:
        comparator = new FloatComparator();
        break;
      case DOUBLE:
        comparator = new DoubleComparator();
        break;
      default:
        comparator = new NullComparator();
        break;
    }
  }

  @Override
  public boolean[] filter( final IFilter filter , final boolean[] filterArray ) throws IOException{
    switch( filter.getFilterType() ){
      case NUMBER:
        NumberFilter numberFilter = (NumberFilter)filter;
        switch( numberFilter.getNumberFilterType() ){
          case EQUAL:
            return toColumnList( comparator.getEqual( dicManager , dicIndexIntBuffer , numberFilter ) , filterArray );
          case NOT_EQUAL:
            return toColumnList( comparator.getNotEqual( dicManager , dicIndexIntBuffer , numberFilter ) , filterArray );
          case LT:
            return toColumnList( comparator.getLt( dicManager , dicIndexIntBuffer , numberFilter ) , filterArray );
          case LE:
            return toColumnList( comparator.getLe( dicManager , dicIndexIntBuffer , numberFilter ) , filterArray );
          case GT:
            return toColumnList( comparator.getGt( dicManager , dicIndexIntBuffer , numberFilter ) , filterArray );
          case GE:
            return toColumnList( comparator.getGe( dicManager , dicIndexIntBuffer , numberFilter ) , filterArray );
          default:
            return null;
        }
      case NUMBER_RANGE:
        NumberRangeFilter numberRangeFilter = (NumberRangeFilter)filter;
        return toColumnList( comparator.getRange( dicManager , dicIndexIntBuffer , numberRangeFilter ) , filterArray );
      default:
        return null;
    }
  }

  private boolean[] toColumnList( final Set<Integer> targetDicSet , final boolean[] filterArray ){
    if( targetDicSet == null ){
      return null;
    }
    if( targetDicSet.isEmpty() ){
      return filterArray;
    }
    int length = dicIndexIntBuffer.capacity();
    List<Integer> result = new ArrayList<Integer>( length );
    for( int i = 0 ; i < length ; i++ ){
      Integer dicIndex = Integer.valueOf( dicIndexIntBuffer.get(i) );
      if( targetDicSet.contains( dicIndex ) ){
        filterArray[i] = true;
      }
    }
    return filterArray;
  }

  public interface IComparator{

    Set<Integer> getEqual( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException;

    Set<Integer> getNotEqual( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException;

    Set<Integer> getLt( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException;

    Set<Integer> getLe( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException;

    Set<Integer> getGt( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException;

    Set<Integer> getGe( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException;

    Set<Integer> getRange( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberRangeFilter numberRangeFilter ) throws IOException;

  }

  public class NullComparator implements IComparator{

    @Override
    public Set<Integer> getEqual( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      return null;
    }

    @Override
    public Set<Integer> getNotEqual( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      return null;
    }

    @Override
    public Set<Integer> getLt( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      return null;
    }

    @Override
    public Set<Integer> getLe( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      return null;
    }

    @Override
    public Set<Integer> getGt( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      return null;
    }

    @Override
    public Set<Integer> getGe( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      return null;
    }

    @Override
    public Set<Integer> getRange( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberRangeFilter numberRangeFilter ) throws IOException{
      return null;
    }

  }

  public class LongComparator implements IComparator{

    @Override
    public Set<Integer> getEqual( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      long target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getLong();
      }catch( NumberFormatException e ){
        return matchDicList;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( target == numObj.getLong() ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getNotEqual( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      long target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getLong();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null || target != numObj.getLong() ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getLt( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      long target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getLong();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( numObj.getLong() < target ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getLe( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      long target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getLong();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( numObj.getLong() <= target ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getGt( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      long target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getLong();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( target < numObj.getLong() ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getGe( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      long target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getLong();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( target <= numObj.getLong() ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getRange( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberRangeFilter numberRangeFilter ) throws IOException{
      Set<Integer> matchDicList = new HashSet<Integer>();
      boolean invert = numberRangeFilter.isInvert();
      long min;
      long max;
      try{
        min = numberRangeFilter.getMinObject().getLong();
        max = numberRangeFilter.getMaxObject().getLong();
      }catch( NumberFormatException e ){
        return null;
      }
      boolean minHasEquals = numberRangeFilter.isMinHasEquals();
      boolean maxHasEquals = numberRangeFilter.isMaxHasEquals();
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        long target = numObj.getLong();
        if( NumberUtils.range( min , minHasEquals , max , maxHasEquals , target ) != invert ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }
      return matchDicList;
    }

  }

  public class IntegerComparator implements IComparator{

    @Override
    public Set<Integer> getEqual( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      int target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getInt();
      }catch( NumberFormatException e ){
        return matchDicList;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( target == numObj.getInt() ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getNotEqual( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      int target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getInt();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null || target != numObj.getInt() ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getLt( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      int target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getInt();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( numObj.getInt() < target ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getLe( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      int target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getInt();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( numObj.getInt() <= target ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getGt( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      int target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getInt();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( target < numObj.getInt() ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getGe( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      int target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getInt();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( target <= numObj.getInt() ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getRange( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberRangeFilter numberRangeFilter ) throws IOException{
      Set<Integer> matchDicList = new HashSet<Integer>();
      boolean invert = numberRangeFilter.isInvert();
      int min;
      int max;
      try{
        min = numberRangeFilter.getMinObject().getInt();
        max = numberRangeFilter.getMaxObject().getInt();
      }catch( NumberFormatException e ){
        return null;
      }
      boolean minHasEquals = numberRangeFilter.isMinHasEquals();
      boolean maxHasEquals = numberRangeFilter.isMaxHasEquals();
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        int target = numObj.getInt();
        if( NumberUtils.range( min , minHasEquals , max , maxHasEquals , target ) != invert ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }
      return matchDicList;
    }

  }

  public class ShortComparator implements IComparator{

    @Override
    public Set<Integer> getEqual( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      short target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getShort();
      }catch( NumberFormatException e ){
        return matchDicList;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( target == numObj.getShort() ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getNotEqual( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      short target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getShort();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null || target != numObj.getShort() ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getLt( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      short target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getShort();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( numObj.getShort() < target ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getLe( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      short target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getShort();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( numObj.getShort() <= target ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getGt( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      short target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getShort();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( target < numObj.getShort() ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getGe( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      short target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getShort();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( target <= numObj.getShort() ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getRange( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberRangeFilter numberRangeFilter ) throws IOException{
      Set<Integer> matchDicList = new HashSet<Integer>();
      boolean invert = numberRangeFilter.isInvert();
      short min;
      short max;
      try{
        min = numberRangeFilter.getMinObject().getShort();
        max = numberRangeFilter.getMaxObject().getShort();
      }catch( NumberFormatException e ){
        return null;
      }
      boolean minHasEquals = numberRangeFilter.isMinHasEquals();
      boolean maxHasEquals = numberRangeFilter.isMaxHasEquals();
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        short target = numObj.getShort();
        if( NumberUtils.range( min , minHasEquals , max , maxHasEquals , target ) != invert ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }
      return matchDicList;
    }

  }

  public class ByteComparator implements IComparator{

    @Override
    public Set<Integer> getEqual( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      byte target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getByte();
      }catch( NumberFormatException e ){
        return matchDicList;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( target == numObj.getByte() ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getNotEqual( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      byte target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getByte();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null || target != numObj.getByte() ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getLt( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      byte target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getByte();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( numObj.getByte() < target ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getLe( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      byte target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getByte();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( numObj.getByte() <= target ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getGt( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      byte target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getByte();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( target < numObj.getByte() ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getGe( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      byte target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = numberFilter.getNumberObject().getByte();
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( target <= numObj.getByte() ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getRange( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberRangeFilter numberRangeFilter ) throws IOException{
      Set<Integer> matchDicList = new HashSet<Integer>();
      boolean invert = numberRangeFilter.isInvert();
      byte min;
      byte max;
      try{
        min = numberRangeFilter.getMinObject().getByte();
        max = numberRangeFilter.getMaxObject().getByte();
      }catch( NumberFormatException e ){
        return null;
      }
      boolean minHasEquals = numberRangeFilter.isMinHasEquals();
      boolean maxHasEquals = numberRangeFilter.isMaxHasEquals();
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        byte target = numObj.getByte();
        if( NumberUtils.range( min , minHasEquals , max , maxHasEquals , target ) != invert ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }
      return matchDicList;
    }

  }

  public class FloatComparator implements IComparator{

    @Override
    public Set<Integer> getEqual( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      return null;
    }

    @Override
    public Set<Integer> getNotEqual( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      return null;
    }

    @Override
    public Set<Integer> getLt( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      Float target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = Float.valueOf( numberFilter.getNumberObject().getFloat() );
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( 0 < target.compareTo( numObj.getFloat() ) ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getLe( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      Float target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = Float.valueOf( numberFilter.getNumberObject().getFloat() );
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( 0 <= target.compareTo( numObj.getFloat() ) ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getGt( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      Float target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = Float.valueOf( numberFilter.getNumberObject().getFloat() );
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( target.compareTo( numObj.getFloat() ) < 0 ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getGe( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      Float target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = Float.valueOf( numberFilter.getNumberObject().getFloat() );
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( target.compareTo( numObj.getFloat() ) <= 0 ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getRange( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberRangeFilter numberRangeFilter ) throws IOException{
      Set<Integer> matchDicList = new HashSet<Integer>();
      boolean invert = numberRangeFilter.isInvert();
      Float min;
      Float max;
      try{
        min = Float.valueOf( numberRangeFilter.getMinObject().getFloat() );
        max = Float.valueOf( numberRangeFilter.getMaxObject().getFloat() );
      }catch( NumberFormatException e ){
        return null;
      }
      boolean minHasEquals = numberRangeFilter.isMinHasEquals();
      boolean maxHasEquals = numberRangeFilter.isMaxHasEquals();
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        Float target = Float.valueOf( numObj.getFloat() );
        if( NumberUtils.range( min , minHasEquals , max , maxHasEquals , target ) != invert ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }
      return matchDicList;
    }

  }

  public class DoubleComparator implements IComparator{

    @Override
    public Set<Integer> getEqual( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      return null;
    }

    @Override
    public Set<Integer> getNotEqual( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      return null;
    }

    @Override
    public Set<Integer> getLt( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      Double target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = Double.valueOf( numberFilter.getNumberObject().getDouble() );
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( 0 < target.compareTo( numObj.getDouble() ) ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getLe( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      Double target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = Double.valueOf( numberFilter.getNumberObject().getDouble() );
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( 0 <= target.compareTo( numObj.getDouble() ) ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getGt( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      Double target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = Double.valueOf( numberFilter.getNumberObject().getDouble() );
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( target.compareTo( numObj.getDouble() ) < 0 ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getGe( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberFilter numberFilter ) throws IOException{
      Double target;
      Set<Integer> matchDicList = new HashSet<Integer>();
      try{
        target = Double.valueOf( numberFilter.getNumberObject().getDouble() );
      }catch( NumberFormatException e ){
        return null;
      }
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        if( target.compareTo( numObj.getDouble() ) <= 0 ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }

      return matchDicList;
    }

    @Override
    public Set<Integer> getRange( final IDicManager dicManager , final IntBuffer dicIndexIntBuffer , final NumberRangeFilter numberRangeFilter ) throws IOException{
      Set<Integer> matchDicList = new HashSet<Integer>();
      boolean invert = numberRangeFilter.isInvert();
      Double min;
      Double max;
      try{
        min = Double.valueOf( numberRangeFilter.getMinObject().getDouble() );
        max = Double.valueOf( numberRangeFilter.getMaxObject().getDouble() );
      }catch( NumberFormatException e ){
        return null;
      }
      boolean minHasEquals = numberRangeFilter.isMinHasEquals();
      boolean maxHasEquals = numberRangeFilter.isMaxHasEquals();
      for( int i = 0 ; i < dicManager.getDicSize() ; i++ ){
        PrimitiveObject numObj = dicManager.get( i );
        if( numObj == null ){
          continue;
        }
        Double target = Double.valueOf( numObj.getDouble() );
        if( NumberUtils.range( min , minHasEquals , max , maxHasEquals , target ) != invert ){
          matchDicList.add( Integer.valueOf( i ) );
        }
      }
      return matchDicList;
    }

  }

}
