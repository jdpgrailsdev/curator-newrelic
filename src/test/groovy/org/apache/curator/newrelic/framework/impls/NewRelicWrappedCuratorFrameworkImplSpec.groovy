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
package org.apache.curator.newrelic.framework.impls

import org.apache.curator.NewRelicWrappedCuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.imps.CuratorFrameworkImpl
import org.apache.curator.newrelic.framework.imps.NewRelicWrappedCuratorFrameworkImpl
import org.apache.curator.retry.RetryOneTime
import org.apache.curator.test.TestingServer

import spock.lang.Shared
import spock.lang.Specification

class NewRelicWrappedCuratorFrameworkImplSpec extends Specification {

    @Shared
    TestingServer server

    def setupSpec() {
        server = new TestingServer()
    }

    def cleanupSpec() {
        server.stop()
    }

    def "test creating a NewRelic wrapped CuratorFramework client from a delegate CuratorFramework client"() {
        when:
            def client = new NewRelicWrappedCuratorFrameworkImpl(CuratorFrameworkFactory.newClient(server.connectString, new RetryOneTime(0)))
        then:
            client != null
            client instanceof NewRelicWrappedCuratorFrameworkImpl
            client.delegate instanceof CuratorFrameworkImpl
            client.getZookeeperClient().currentConnectionString == server.connectString
            client.getZookeeperClient().retryPolicy instanceof RetryOneTime
        cleanup:
            client.close()
    }

    def "test creating a NewRelic wrapped CuratorFramework client from a null delegate"() {
        when:
            new NewRelicWrappedCuratorFrameworkImpl(null)
        then:
            thrown(IllegalArgumentException)
    }

    def "test retrieving the NewRelic wrapped ZooKeeper client"() {
        setup:
            def client = new NewRelicWrappedCuratorFrameworkImpl(CuratorFrameworkFactory.newClient(server.connectString, new RetryOneTime(0)))
        when:
            def zkClient = client.getZookeeperClient()
        then:
            zkClient != null
            zkClient instanceof NewRelicWrappedCuratorZookeeperClient
        cleanup:
            client.close()
    }

    def "test retrieving the NewRelic wrapped ZooKeeper client when an exception occurs"() {
        setup:
            CuratorFramework delegate = Mock() {
                getZookeeperClient() >> { throw new IllegalArgumentException('test') }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            def zkClient = client.getZookeeperClient()
        then:
            thrown IllegalArgumentException
    }
}