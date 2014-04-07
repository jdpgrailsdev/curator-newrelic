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

    def "test that calls to start() invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                start() >> {}
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.start()
        then:
            1 * delegate.start()
    }

    def "test that calls to close() invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                close() >> {}
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.close()
        then:
            1 * delegate.close()
    }

    def "test that calls to getState() invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                getState() >> {}
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.getState()
        then:
            1 * delegate.getState()
    }

    def "test that calls to isStarted() invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                isStarted() >> { false }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.isStarted()
        then:
            1 * delegate.isStarted()
    }

    def "test that calls to create() invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                create() >> { null }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.create()
        then:
            1 * delegate.create()
    }

    def "test that calls to delete() invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                delete() >> { null }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.delete()
        then:
            1 * delegate.delete()
    }

    def "test that calls to checkExists() invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                checkExists() >> { null }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.checkExists()
        then:
            1 * delegate.checkExists()
    }

    def "test that calls to getData() invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                getData() >> { null }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.getData()
        then:
            1 * delegate.getData()
    }

    def "test that calls to setData() invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                setData() >> { null }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.setData()
        then:
            1 * delegate.setData()
    }

    def "test that calls to getChildren() invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                getChildren() >> { null }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.getChildren()
        then:
            1 * delegate.getChildren()
    }

    def "test that calls to getACL() invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                getACL() >> { null }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.getACL()
        then:
            1 * delegate.getACL()
    }

    def "test that calls to setACL() invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                setACL() >> {  }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.setACL()
        then:
            1 * delegate.setACL()
    }

    def "test that calls to inTransaction() invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                inTransaction() >> { null }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.inTransaction()
        then:
            1 * delegate.inTransaction()
    }

    def "test that calls to sync(String,Object) invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                sync(_,_) >> { null }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.sync('path', new Object())
        then:
            1 * delegate.sync(_,_)
    }

    def "test that calls to sync() invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                sync() >> { null }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.sync()
        then:
            1 * delegate.sync()
    }

    def "test that calls to getConnectionStateListenable() invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                getConnectionStateListenable() >> { null }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.getConnectionStateListenable()
        then:
            1 * delegate.getConnectionStateListenable()
    }

    def "test that calls to getCuratorListenable() invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                getCuratorListenable() >> { null }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.getCuratorListenable()
        then:
            1 * delegate.getCuratorListenable()
    }

    def "test that calls to getUnhandledErrorListenable() invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                getUnhandledErrorListenable() >> { null }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.getUnhandledErrorListenable()
        then:
            1 * delegate.getUnhandledErrorListenable()
    }

    def "test that calls to nonNamespaceView() invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                nonNamespaceView() >> { null }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.nonNamespaceView()
        then:
            1 * delegate.nonNamespaceView()
    }

    def "test that calls to usingNamespace(String) invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                usingNamespace(_) >> { null }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.usingNamespace('namespace')
        then:
            1 * delegate.usingNamespace(_)
    }

    def "test that calls to getNamespace() invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                getNamespace(_) >> { null }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.getNamespace()
        then:
            1 * delegate.getNamespace()
    }

    def "test that calls to newNamespaceAwareEnsurePath(String) invoke the underlying delegate"() {
        setup:
            CuratorFramework delegate = Mock() {
                newNamespaceAwareEnsurePath(_) >> { null }
            }
            def client = new NewRelicWrappedCuratorFrameworkImpl(delegate)
        when:
            client.newNamespaceAwareEnsurePath('path')
        then:
            1 * delegate.newNamespaceAwareEnsurePath(_)
    }
}