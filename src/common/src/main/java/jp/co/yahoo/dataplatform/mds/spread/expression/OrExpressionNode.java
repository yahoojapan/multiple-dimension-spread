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
package jp.co.yahoo.dataplatform.mds.spread.expression;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;

import jp.co.yahoo.dataplatform.mds.spread.Spread;
import jp.co.yahoo.dataplatform.mds.util.CollectionUtils;

import jp.co.yahoo.dataplatform.mds.binary.blockindex.BlockIndexNode;

public class OrExpressionNode implements IExpressionNode {

  private final List<IExpressionNode> childNode = new ArrayList<IExpressionNode>();
  private final long denominator;

  public OrExpressionNode(){
    denominator = 2;
  }

  public OrExpressionNode( final double pushdownRate ){
    if( pushdownRate <= (double)0 ){
      denominator = 0;
    }
    else if( (double)1 <= pushdownRate ){
      denominator  = 1;
    }
    else{
      denominator = Double.valueOf( (double)1 / pushdownRate ).longValue();
    }
  }

  @Override
  public void addChildNode( final IExpressionNode node ){
    childNode.add( node );
  }

  @Override
  public List<Integer> exec( final Spread spread ) throws IOException{
    return exec( spread , null );
  }

  @Override
  public List<Integer> exec( final Spread spread , final List<Integer> parentList ) throws IOException{
    if( denominator == 0 ){
      return null;
    }
    List<Integer> union = parentList;
    long min = spread.size() / denominator;
    for( IExpressionNode node : childNode ){
      List<Integer> result = node.exec( spread , null );
      if( result != null && min < result.size() ){
        result = null;
      }
      if( result == null ){
        return null;
      }
      if( union == null ){
        union = result;
      }
      else{
        union = CollectionUtils.unionFromSortedCollection( union , result );
      }

      if( spread.size() == union.size() ){
        return union;
      }
    }

    return union;
  }

  @Override
  public boolean canBlockSkip( final BlockIndexNode indexNode ) throws IOException{
    if( childNode.isEmpty() ){
      return false;
    }
    for( IExpressionNode node : childNode ){
      if( ! node.canBlockSkip( indexNode ) ){
        return false;
      }
    }

    return true;
  }

}
