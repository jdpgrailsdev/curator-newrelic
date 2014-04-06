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
package org.apache.curator.newrelic.framework

import org.apache.curator.newrelic.framework.imps.NewRelicWrappedCuratorFrameworkImpl
import org.apache.curator.retry.RetryOneTime
import org.apache.curator.test.TestingServer

import spock.lang.Shared
import spock.lang.Specification

class NewRelicClientFrameworkFactorySpec extends Specification {

	@Shared
	TestingServer server

	def setupSpec() {
		server = new TestingServer()
	}

	def cleanupSpec() {
		server.stop()
	}

	def "test creating a NewRelic wrapped CuratorFramework client for a connection string and retry policy"() {
		when:
		def client = NewRelicClientFrameworkFactory.newClient(server.connectString, new RetryOneTime(0))
		then:
		client != null
		client instanceof NewRelicWrappedCuratorFrameworkImpl
		cleanup:
		client.close()
	}

	def "test creating a NewRelic wrapped CuratorFramework client for a connection string, timeouts and retry policy"() {
		when:
		def client = NewRelicClientFrameworkFactory.newClient(server.connectString, 10, 30, new RetryOneTime(0))
		then:
		client != null
		client instanceof NewRelicWrappedCuratorFrameworkImpl
		cleanup:
		client.close()
	}
}
