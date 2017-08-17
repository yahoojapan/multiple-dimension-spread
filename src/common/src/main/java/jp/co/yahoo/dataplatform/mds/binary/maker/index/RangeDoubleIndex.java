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

import java.util.List;
import java.util.ArrayList;

import jp.co.yahoo.dataplatform.mds.spread.column.filter.IFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.NumberFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.NumberRangeFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.index.ICellIndex;

public class RangeDoubleIndex implements ICellIndex{

  private final Double min;
  private final Double max;

  public RangeDoubleIndex( final Double min , final Double max ){
    this.min = min;
    this.max = max;
  }

  @Override
  public List<Integer> filter( final IFilter filter ) throws IOException{
    switch( filter.getFilterType() ){
      case NUMBER:
        NumberFilter numberFilter = (NumberFilter)filter;
        Double setNumber;
        try{
          setNumber = Double.valueOf( numberFilter.getNumberObject().getDouble() );
        }catch( NumberFormatException e ){
          return null;
        }
        switch( numberFilter.getNumberFilterType() ){
          case EQUAL:
            if( 0 < min.compareTo( setNumber ) || max.compareTo( setNumber ) < 0 ){
              return new ArrayList<Integer>();
            }
            return null;
          case LT:
            if( 0 <= min.compareTo( setNumber ) ){
              return new ArrayList<Integer>();
            }
            return null;
          case LE:
            if( 0 < min.compareTo( setNumber ) ){
              return new ArrayList<Integer>();
            }
            return null;
          case GT:
            if( max.compareTo( setNumber ) <= 0 ){
              return new ArrayList<Integer>();
            }
            return null;
          case GE:
            if( max.compareTo( setNumber ) < 0 ){
              return new ArrayList<Integer>();
            }
            return null;
          default:
            return null;
        }
      case NUMBER_RANGE:
        NumberRangeFilter numberRangeFilter = (NumberRangeFilter)filter;
        Double setMin;
        Double setMax;
        try{
          setMin = Double.valueOf( numberRangeFilter.getMinObject().getDouble() );
          setMax = Double.valueOf( numberRangeFilter.getMaxObject().getDouble() );
        }catch( NumberFormatException e ){
          return null;
        }
        boolean minHasEquals = numberRangeFilter.isMinHasEquals();
        boolean maxHasEquals = numberRangeFilter.isMaxHasEquals();
        boolean invert = numberRangeFilter.isInvert();
        if( minHasEquals && maxHasEquals ){
          if( ( 0 < min.compareTo( setMax ) || max.compareTo( setMin ) < 0 ) != invert ){
            return new ArrayList<Integer>();
          }
          return null;
        }
        else if( minHasEquals ){
          if( ( 0 < min.compareTo( setMax ) || max.compareTo( setMin ) <= 0 ) != invert ){
            return new ArrayList<Integer>();
          }
          return null;
        }
        else if( maxHasEquals ){
          if( ( 0 <= min.compareTo( setMax ) || max.compareTo( setMin ) < 0 ) != invert ){
            return new ArrayList<Integer>();
          }
          return null;
        }
        else{
          if( ( 0 <= min.compareTo( setMax ) || max.compareTo( setMin ) <= 0 ) != invert ){
            return new ArrayList<Integer>();
          }
          return null;
        }
      default:
        return null;
    }
  }

}
