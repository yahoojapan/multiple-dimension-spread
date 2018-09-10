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
package jp.co.yahoo.dataplatform.mds.tools;

import java.lang.management.*;

public final class CpuTimeUtil{

  public static final double TO_MILL = 1000 * 1000;

  public static class CpuTime{

    public long cpuTimeNano;
    public long userTimeNano;
    public long sysTimeNano;

  }

  public static CpuTime getCurrentCpuTime( long id ){
    CpuTime result = new CpuTime();
    ThreadMXBean bean = ManagementFactory.getThreadMXBean();
    if ( ! bean.isThreadCpuTimeSupported() ){
      return result;
    }
    result.cpuTimeNano = bean.getThreadCpuTime( id );
    result.userTimeNano = bean.getThreadUserTime( id );
    result.sysTimeNano = bean.getThreadUserTime( id ) - bean.getThreadCpuTime( id );

    return result;
  }

  public static double nanoToMill( final long num ){
    return (double)num / TO_MILL;
  }

  public static long calcCpuTimeNano( final CpuTime start , final CpuTime end ){
    return end.cpuTimeNano - start.cpuTimeNano;
  }

  public static long calcUserTimeNano( final CpuTime start , final CpuTime end ){
    return end.userTimeNano - start.userTimeNano;
  }

  public static long calcSysTimeNano( final CpuTime start , final CpuTime end ){
    return end.sysTimeNano - start.sysTimeNano;
  }

}
