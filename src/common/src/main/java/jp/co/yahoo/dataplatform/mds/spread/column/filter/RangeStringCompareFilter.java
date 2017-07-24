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
package jp.co.yahoo.dataplatform.mds.spread.column.filter;

public class RangeStringCompareFilter implements IStringCompareFilter{

  private final IStringCompareFilter minFilter;
  private final IStringCompareFilter maxFilter;

  public RangeStringCompareFilter( final String min , final boolean minHasEqual , final String max , final boolean maxHasEqual ){
    if( minHasEqual ){
      minFilter = new GtStringCompareFilter( min );
    }
    else{
      minFilter = new GeStringCompareFilter( min );
    }
    if( maxHasEqual ){
      maxFilter = new LtStringCompareFilter( max );
    }
    else{
      maxFilter = new LeStringCompareFilter( max );
    }
  }

  @Override
  public IStringComparator getStringComparator(){
    return new RangeStringComparator( minFilter.getStringComparator() , maxFilter.getStringComparator() );
  }

  @Override
  public StringCompareFilterType getStringCompareFilterType(){
    return StringCompareFilterType.RANGE;
  }

  @Override
  public FilterType getFilterType(){
    return FilterType.STRING_COMPARE;
  }

  private class RangeStringComparator implements IStringComparator{

    private final IStringComparator min;
    private final IStringComparator max;

    public RangeStringComparator( final IStringComparator min , final IStringComparator max ){
      this.min = min;
      this.max = max;
    }

    @Override
    public boolean isFilterString( final String target ){
      if( min.isFilterString( target ) ){
        return true;
      }
      if( max.isFilterString( target ) ){
        return true;
      }
      return false;
    }

  }

}
