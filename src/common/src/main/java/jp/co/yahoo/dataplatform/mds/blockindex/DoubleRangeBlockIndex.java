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

import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import java.util.Set;

import jp.co.yahoo.dataplatform.mds.constants.PrimitiveByteLength;

import jp.co.yahoo.dataplatform.mds.spread.column.filter.IFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.NumberFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.NumberRangeFilter;

public class DoubleRangeBlockIndex implements IBlockIndex{

  private Double min;
  private Double max;

  public DoubleRangeBlockIndex(){
    min = Double.MAX_VALUE;
    max = Double.MIN_VALUE;
  }

  public DoubleRangeBlockIndex( final Double min , final Double max ){
    this.min = min;
    this.max = max;
  }

  @Override
  public BlockIndexType getBlockIndexType(){
    return BlockIndexType.RANGE_DOUBLE;
  }

  @Override
  public boolean merge( final IBlockIndex blockIndex ){
    if( ! ( blockIndex instanceof DoubleRangeBlockIndex ) ){
      return false;
    }
    DoubleRangeBlockIndex numberBlockIndex = (DoubleRangeBlockIndex)blockIndex;
    if( numberBlockIndex.getMin() < min ){
      min = numberBlockIndex.getMin();
    }
    if( max < numberBlockIndex.getMax() ){
      max = numberBlockIndex.getMax();
    }
    return true;
  }

  @Override
  public int getBinarySize(){
    return PrimitiveByteLength.DOUBLE_LENGTH * 2;
  }

  @Override
  public byte[] toBinary(){
    byte[] result = new byte[getBinarySize()];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    wrapBuffer.putDouble( min );
    wrapBuffer.putDouble( max );
    return result;
  }

  @Override
  public void setFromBinary( final byte[] buffer , final int start , final int length ){
    ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer );
    min = wrapBuffer.getDouble();
    max = wrapBuffer.getDouble();
  }

  @Override
  public boolean canBlockSkip( final IFilter filter ){
    switch( filter.getFilterType() ){
      case NUMBER:
        NumberFilter numberFilter = (NumberFilter)filter;
        Double setNumber;
        try{
          setNumber = Double.valueOf( numberFilter.getNumberObject().getDouble() );
        }catch( NumberFormatException|IOException e ){
          return false;
        }
        switch( numberFilter.getNumberFilterType() ){
          case EQUAL:
            if( 0 < min.compareTo( setNumber ) || max.compareTo( setNumber ) < 0 ){
              return true;
            }
            return false;
          case LT:
            if( 0 <= min.compareTo( setNumber ) ){
              return true;
            }
            return false;
          case LE:
            if( 0 < min.compareTo( setNumber ) ){
              return true;
            }
            return false;
          case GT:
            if( max.compareTo( setNumber ) <= 0 ){
              return true;
            }
            return false;
          case GE:
            if( max.compareTo( setNumber ) < 0 ){
              return true;
            }
            return false;
          default:
            return false;
        }
      case NUMBER_RANGE:
        NumberRangeFilter numberRangeFilter = (NumberRangeFilter)filter;
        Double setMin;
        Double setMax;
        try{
          setMin = Double.valueOf( numberRangeFilter.getMinObject().getDouble() );
          setMax = Double.valueOf( numberRangeFilter.getMaxObject().getDouble() );
        }catch( NumberFormatException|IOException e ){
          return false;
        }
        boolean minHasEquals = numberRangeFilter.isMinHasEquals();
        boolean maxHasEquals = numberRangeFilter.isMaxHasEquals();
        boolean invert = numberRangeFilter.isInvert();
        if( minHasEquals && maxHasEquals ){
          if( ( 0 < min.compareTo( setMax ) || max.compareTo( setMin ) < 0 ) != invert ){
            return true;
          }
          return false;
        }
        else if( minHasEquals ){
          if( ( 0 < min.compareTo( setMax ) || max.compareTo( setMin ) <= 0 ) != invert ){
            return true;
          }
          return false;
        }
        else if( maxHasEquals ){
          if( ( 0 <= min.compareTo( setMax ) || max.compareTo( setMin ) < 0 ) != invert ){
            return true;
          }
          return false;
        }
        else{
          if( ( 0 <= min.compareTo( setMax ) || max.compareTo( setMin ) <= 0 ) != invert ){
            return true;
          }
          return false;
        }
      default:
        return false;
    }
  }

  @Override
  public IBlockIndex getNewInstance(){
    return new DoubleRangeBlockIndex();
  }

  public Double getMin(){
    return min;
  }

  public Double getMax(){
    return max;
  }

}
