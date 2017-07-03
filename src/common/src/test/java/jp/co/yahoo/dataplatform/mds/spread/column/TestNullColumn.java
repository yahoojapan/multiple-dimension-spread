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
package jp.co.yahoo.dataplatform.mds.spread.column;

import jp.co.yahoo.dataplatform.mds.spread.Spread;
import org.testng.annotations.Test;

import jp.co.yahoo.dataplatform.schema.objects.StringObj;

import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestNullColumn {

    @Test
    public void T_getColumnName() {
        assertEquals(NullColumn.getInstance().getColumnName(), ColumnType.NULL.toString());
    }
    
    @Test
    public void T_getColumnType() {
        assertEquals(NullColumn.getInstance().getColumnType(), ColumnType.NULL);
    }
    
    @Test
    public void T_set_get_ParentsColumn() throws IOException {
        Spread spread = new Spread();
        Map<String,Object> dataContainer = new LinkedHashMap<String,Object>();
        dataContainer.put("parents_column_key", new StringObj("val"));
        spread.addRow( dataContainer );
        IColumn icolumn = spread.getColumn("parents_column_key");

        IColumn target = NullColumn.getInstance();
        target.setParentsColumn(icolumn);
        //TODO あとで仕様確認しても良いかも
        assertEquals(target.getParentsColumn().getColumnName(), "parents_column_key");
    }
    
    @Test
    public void T_add() throws IOException {
        assertEquals(NullColumn.getInstance().add(ColumnType.STRING, new StringObj("string"), 1), 0);
    }

}
