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

import jp.co.yahoo.dataplatform.mds.spread.column.filter.IFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.NumberFilter;
import jp.co.yahoo.dataplatform.mds.spread.column.filter.NumberRangeFilter;

public class ByteRangeBlockIndex implements IBlockIndex{

  private byte min;
  private byte max;

  public ByteRangeBlockIndex(){
    min = Byte.MAX_VALUE;
    max = Byte.MIN_VALUE;
  }

  public ByteRangeBlockIndex( final byte min , final byte max ){
    this.min = min;
    this.max = max;
  }

  @Override
  public BlockIndexType getBlockIndexType(){
    return BlockIndexType.RANGE_BYTE;
  }

  @Override
  public boolean merge( final IBlockIndex blockIndex ){
    if( ! ( blockIndex instanceof ByteRangeBlockIndex ) ){
      return false;
    }
    ByteRangeBlockIndex numberBlockIndex = (ByteRangeBlockIndex)blockIndex;
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
    return Byte.BYTES * 2;
  }

  @Override
  public byte[] toBinary(){
    byte[] result = new byte[getBinarySize()];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    wrapBuffer.put( min );
    wrapBuffer.put( max );
    return result;
  }

  @Override
  public void setFromBinary( final byte[] buffer , final int start , final int length ){
    ByteBuffer wrapBuffer = ByteBuffer.wrap( buffer );
    min = wrapBuffer.get();
    max = wrapBuffer.get();
  }

  @Override
  public boolean canBlockSkip( final IFilter filter ){
    switch( filter.getFilterType() ){
      case NUMBER:
        NumberFilter numberFilter = (NumberFilter)filter;
        byte setNumber;
        try{
          setNumber = numberFilter.getNumberObject().getByte();
        }catch( NumberFormatException|IOException e ){
          return false;
        }
        switch( numberFilter.getNumberFilterType() ){
          case EQUAL:
            if( setNumber < min || max < setNumber ){
              return true;
            }
            return false;
          case LT:
            if( setNumber <= min ){
              return true;
            }
            return false;
          case LE:
            if( setNumber < min ){
              return true;
            }
            return false;
          case GT:
            if( max <= setNumber ){
              return true;
            }
            return false;
          case GE:
            if( max < setNumber ){
              return true;
            }
            return false;
          default:
            return false;
        }
      case NUMBER_RANGE:
        NumberRangeFilter numberRangeFilter = (NumberRangeFilter)filter;
        byte setMin;
        byte setMax;
        try{
          setMin = numberRangeFilter.getMinObject().getByte();
          setMax = numberRangeFilter.getMaxObject().getByte();
        }catch( NumberFormatException|IOException e ){
          return false;
        }
        boolean minHasEquals = numberRangeFilter.isMinHasEquals();
        boolean maxHasEquals = numberRangeFilter.isMaxHasEquals();
        boolean invert = numberRangeFilter.isInvert();
        if( minHasEquals && maxHasEquals ){
          if( ( setMax < min || max < setMin ) != invert ){
            return true;
          }
          return false;
        }
        else if( minHasEquals ){
          if( ( setMax < min || max <= setMin ) != invert ){
            return true;
          }
          return false;
        }
        else if( maxHasEquals ){
          if( ( setMax <= min || max < setMin ) != invert ){
            return true;
          }
          return false;
        }
        else{
          if( ( setMax <= min || max <= setMin ) != invert ){
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
    return new ByteRangeBlockIndex();
  }

  public byte getMin(){
    return min;
  }

  public byte getMax(){
    return max;
  }

}
