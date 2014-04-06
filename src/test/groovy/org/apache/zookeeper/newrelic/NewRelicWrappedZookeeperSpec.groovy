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
package org.apache.zookeeper.newrelic

import org.apache.curator.test.TestingServer
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooKeeper

import spock.lang.Shared
import spock.lang.Specification

class NewRelicWrappedZookeeperSpec extends Specification {

    @Shared
    TestingServer server

    def setupSpec() {
        server = new TestingServer()
    }

    def cleanupSpec() {
        server.stop()
    }

    def "test creating the NewRelic wrapped ZooKeeper client from a delegate"() {
        setup:
            Watcher watcher = Mock()
            ZooKeeper delegate = new ZooKeeper(server.connectString, 10, watcher)
        when:
            def wrappedZk = new NewRelicWrappedZookeeper(delegate, server.connectString)
        then:
            wrappedZk != null
            wrappedZk instanceof NewRelicWrappedZookeeper
        cleanup:
            wrappedZk.close()
    }
}
