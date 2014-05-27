/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.curator

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.newrelic.framework.NewRelicClientFrameworkFactory
import org.apache.curator.retry.RetryOneTime
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.newrelic.NewRelicWrappedZookeeper

import spock.lang.Shared
import spock.lang.Specification

class NewRelicWrappedCuratorZookeeperClientSpec extends Specification {

    @Shared
    TestingServer server

    @Shared
    CuratorFramework client

    def setupSpec() {
        server = new TestingServer()
        client = NewRelicClientFrameworkFactory.newClient(server.connectString, new RetryOneTime(0))
    }

    def cleanupSpec() {
        client.close()
        server.stop()
    }

    def "test the creation of a New Relic wrapped Curator ZooKeeper client from a delegate"() {
        when:
            def wrappedClient = new NewRelicWrappedCuratorZookeeperClient(client.getZookeeperClient())
        then:
            notThrown(Exception)
            wrappedClient != null
        cleanup:
            wrappedClient.close()
    }

    def "test retrieving the ZooKeeper client from the New Relic wrapped Curator ZooKeeper client"() {
        setup:
            def wrappedClient = new NewRelicWrappedCuratorZookeeperClient(client.getZookeeperClient())
        when:
            def wrappedZkClient = wrappedClient.getZooKeeper()
        then:
            wrappedZkClient != null
            wrappedZkClient instanceof NewRelicWrappedZookeeper
        cleanup:
            wrappedClient.close()
    }
}
