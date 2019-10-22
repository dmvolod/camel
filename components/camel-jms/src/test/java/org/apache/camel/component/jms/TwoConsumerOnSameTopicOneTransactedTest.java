/*
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
package org.apache.camel.component.jms;

import javax.jms.ConnectionFactory;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.spring.CamelSpringTestSupport;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class TwoConsumerOnSameTopicOneTransactedTest extends CamelSpringTestSupport {

    @Override
    protected ClassPathXmlApplicationContext createApplicationContext() {
        return new ClassPathXmlApplicationContext(
                "/org/apache/camel/component/jms/tx/MultipleTopicsTransactedTest.xml");
    }

    @Test
    public void testMultipleMessagesOnSameTopic() throws Exception {
        // give a bit of time for AMQ to properly setup topic subscribers
        Thread.sleep(500);

        //getMockEndpoint("mock:a").expectedBodiesReceived("Hello Camel 1", "Hello Camel 2", "Hello Camel 3", "Hello Camel 4");
        //getMockEndpoint("mock:b").expectedBodiesReceived("Hello Camel 1", "Hello Camel 2", "Hello Camel 3", "Hello Camel 4");
        getMockEndpoint("mock:b").expectedMessageCount(4);

        template.sendBody("activemq:topic:foo", "Hello Camel 1");
        template.sendBody("activemq:topic:foo", "Hello Camel 2");
        template.sendBody("activemq:topic:foo", "Hello Camel 3");
        template.sendBody("activemq:topic:foo", "Hello Camel 4");

        assertMockEndpointsSatisfied();
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("activemq:topic:foo").routeId("a")
                    .to("log:a", "mock:a");

                from("activemq-tr:topic:foo?acknowledgementModeName=SESSION_TRANSACTED").routeId("b")
                    .to("log:b", "mock:b");
            }
        };
    }
}
