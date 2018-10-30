/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.sql;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.ExchangePattern;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

/**
 * @version 
 */
public class SqlProducerOnDuplicateKeyTest extends CamelTestSupport {

    private EmbeddedDatabase db;

    @Before
    public void setUp() throws Exception {
        db = new EmbeddedDatabaseBuilder()
            .setType(EmbeddedDatabaseType.HSQL).addScript("sql/createAndPopulateDatabase7.sql").build();

        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        
        db.shutdown();
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testNamedParametersFromHeaders() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:result");
        mock.expectedMessageCount(1);

        Map<String, Object> map = new HashMap<>();
        map.put("prj", "Camel");
        map.put("lic", "ASF");
        map.put("bk", "Camel in Action2");

        template.sendBodyAndHeaders("direct:start", "This is a dummy body", map);

        mock.assertIsSatisfied();
        
        List list = (List)template.sendBodyAndHeaders("direct:get", ExchangePattern.InOut, "This is a dummy body", map);
        Map row = (Map) list.get(0);
        assertEquals(1, list.size());
        assertEquals("Camel in Action2", row.get("BOOK"));
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            public void configure() {
                getContext().getComponent("sql", SqlComponent.class).setDataSource(db);

                from("direct:start")
                    .to("sql:insert into projects (`project`, `license`, book) values (:#prj, :#lic, :#bk) on duplicate key update `project`=values(`project`), `license`=values(`license`), book = :#bk?batch=true")
                    .to("mock:result");
                
                from("direct:get")
                    .to("sql:select * from projects where `project` = :#prj and `license` = :#lic");
            }
        };
    }
}
