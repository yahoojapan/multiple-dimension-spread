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

import jp.co.yahoo.dataplatform.mds.blockindex.BlockIndexNode;

public class NotExpressionNode implements IExpressionNode {

  private IExpressionNode childNode;
  private final long denominator;

  public NotExpressionNode(){
    denominator = 2;
  }

  public NotExpressionNode( final IExpressionNode childNode ){
    this.childNode = childNode;
    denominator = 2;
  }

  public NotExpressionNode( final IExpressionNode childNode , final double pushdownRate ){
    this.childNode = childNode;
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
    this.childNode = node;
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
    List<Integer> childCollection = null;
    if( childNode == null ){
      return null;
    }
    long min = spread.size() / denominator;
    childCollection = childNode.exec( spread );
    if( childCollection != null && min < ( spread.size() - childCollection.size() ) ){
      childCollection = null;
    }
    
    if( childCollection == null ){
      return null;
    }

    if( parentList == null ){
      return createNewReverseList( childCollection , spread.size() );
    }
    else{
      return createReverseList( childCollection , parentList );
    }
  }

  @Override
  public boolean canBlockSkip( final BlockIndexNode indexNode ) throws IOException{
    return false;
  }

  private List<Integer> createNewReverseList( final List<Integer> childCollection , final int spreadSize ){
    List<Integer> result = new ArrayList<Integer>();
    int offset = 0;
    for( Integer removeNumber : childCollection ){
      for( int i = offset ; i < removeNumber.intValue() ; i++ ){
        result.add( Integer.valueOf( i ) );
      }
      offset = removeNumber.intValue() + 1;
    }
    for( int i = offset ; i < spreadSize ; i++ ){
      result.add( Integer.valueOf( i ) );
    }

    return result;
  }

  private List<Integer> createReverseList( final List<Integer> childCollection , final List<Integer> parentList ){
    List<Integer> result = new ArrayList<Integer>();
    int childOffset = 0;
    int childCollectionSize = childCollection.size();
    for( Integer parentIndexObj : parentList ){
      int parentIndex = parentIndexObj.intValue();
      while( childOffset < childCollectionSize && childCollection.get( childOffset ).intValue() < parentIndex ){
        childOffset++;
      }
      if( childOffset == childCollectionSize ){
        result.add( parentIndexObj );
      }
      else if( ! parentIndexObj.equals( childCollection.get( childOffset ) ) ){
        result.add( parentIndexObj );
      }
    }

    return result;
  }

}
