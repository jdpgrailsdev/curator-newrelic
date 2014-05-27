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

import java.util.concurrent.TimeUnit

import org.apache.curator.test.TestingServer
import org.apache.zookeeper.AsyncCallback
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.AsyncCallback.StringCallback
import org.apache.zookeeper.data.ACL
import org.apache.zookeeper.data.Id
import org.apache.zookeeper.data.Stat

import spock.lang.Shared
import spock.lang.Specification

class NewRelicWrappedZookeeperSpec extends Specification {

    @Shared
    TestingServer server

    @Shared
    ZooKeeper delegate

    @Shared
    NewRelicWrappedZookeeper wrappedZk

    def setupSpec() {
        server = new TestingServer()
        delegate = new ZooKeeper(server.connectString, TimeUnit.SECONDS.toMillis(10).intValue(), Mock(Watcher))
        wrappedZk = new NewRelicWrappedZookeeper(delegate, server.connectString)
    }

    def cleanupSpec() {
        wrappedZk.close()
        server.stop()
    }

    def "test creating the New Relic wrapped ZooKeeper client from a delegate"() {
        when:
            //ZooKeeper delegate = new ZooKeeper(server.connectString, TimeUnit.SECONDS.toMillis(1).intValue(), Mock(Watcher))
            def wrappedZk = new NewRelicWrappedZookeeper(delegate, server.connectString)
        then:
            wrappedZk != null
            wrappedZk instanceof NewRelicWrappedZookeeper
        cleanup:
            wrappedZk.close()
    }

    def "test creating data in ZooKeeper"() {
        setup:
            //ZooKeeper delegate = new ZooKeeper(server.connectString, TimeUnit.SECONDS.toMillis(1).intValue(), Mock(Watcher))
            //NewRelicWrappedZookeeper wrappedZk = new NewRelicWrappedZookeeper(delegate, server.connectString)
        when:
            wrappedZk.create('/test', 'test'.getBytes(), [new ACL(ZooDefs.Perms.ALL, new Id())] as List<ACL>, CreateMode.PERSISTENT)
        then:
            new String(wrappedZk.getData('/test', false, new Stat())) == 'test'
        cleanup:
            wrappedZk.close()
    }

    def "test creating data in ZooKeeper with a callback"() {
        setup:
            //ZooKeeper delegate = new ZooKeeper(server.connectString, TimeUnit.SECONDS.toMillis(1).intValue(), Mock(Watcher))
            //NewRelicWrappedZookeeper wrappedZk = new NewRelicWrappedZookeeper(delegate, server.connectString)
        when:
            wrappedZk.create('/test-callback', 'test-callback'.getBytes(), [new ACL(ZooDefs.Perms.ALL, new Id())] as List<ACL>, CreateMode.PERSISTENT, Mock(AsyncCallback.StringCallback), new Object())
        then:
            new String(wrappedZk.getData('/test-callback', false, new Stat())) == 'test-callback'
        cleanup:
            wrappedZk.close()
    }

    def "test deleting data in ZooKeeper"() {
        setup:
            //ZooKeeper delegate = new ZooKeeper(server.connectString, TimeUnit.SECONDS.toMillis(1).intValue(), Mock(Watcher))
           // NewRelicWrappedZookeeper wrappedZk = new NewRelicWrappedZookeeper(delegate, server.connectString)
            wrappedZk.create('/delete', 'delete'.getBytes(), [new ACL(ZooDefs.Perms.ALL, new Id())] as List<ACL>, CreateMode.PERSISTENT)
        when:
            wrappedZk.delete('/delete', 0)
        then:
            wrappedZk.getData('/delete', false, new Stat()) == null
        cleanup:
            wrappedZk.close()
    }

    def "test deleting data in ZooKeeper with a callback"() {
        setup:
            //ZooKeeper delegate = new ZooKeeper(server.connectString, TimeUnit.SECONDS.toMillis(1).intValue(), Mock(Watcher))
            //NewRelicWrappedZookeeper wrappedZk = new NewRelicWrappedZookeeper(delegate, server.connectString)
            wrappedZk.create('/delete-callback', 'delete-callback'.getBytes(), [new ACL(ZooDefs.Perms.ALL, new Id())] as List<ACL>, CreateMode.PERSISTENT)
        when:
            wrappedZk.delete('/delete-callback', 0, Mock(AsyncCallback.VoidCallback))
        then:
            wrappedZk.getData('/delete-callback', false, new Stat()) == null
        cleanup:
            wrappedZk.close()
    }
}