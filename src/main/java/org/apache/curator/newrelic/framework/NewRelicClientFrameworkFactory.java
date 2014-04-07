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
package org.apache.curator.newrelic.framework;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.newrelic.framework.imps.NewRelicWrappedCuratorFrameworkImpl;

/**
 * This factory is intended to be used in place of the {@link CuratorFrameworkFactory} to create
 * {@link CuratorFramework} instances that participate in <a href="http://newrelic.com">New Relic</a> transaction
 * traces.  It is intended to be used as a drop-in replacement wherever the existing {@link CuratorFrameworkFactory}
 * class is currently in use.
 *
 * @author Jonathan Pearlin
 * @since 1.0.0
 * @see CuratorFramework
 * @see CuratorFrameworkFactory
 * @see <a href="http://newrelic.com">New Relic</a>
 */
public class NewRelicClientFrameworkFactory {

	/**
	 * Default session timeout value in milliseconds that can be provided by the {@code curator-default-session-timeout} system property.  Defaults to 60 seconds.
	 */
	public static final int DEFAULT_SESSION_TIMEOUT_MS = Integer.getInteger("curator-default-session-timeout", 60 * 1000);

	/**
	 * Default connection timeout value in milliseconds that can be provided by the {@code curator-default-connection-timeout} system property.  Defaults to 15 seconds.
	 */
	public static final int DEFAULT_CONNECTION_TIMEOUT_MS = Integer.getInteger("curator-default-connection-timeout", 15 * 1000);

	/**
	 * Creates a new client, using the default session timeout and default connection timeout values.
	 * @param connectString The list of servers to connect to.
	 * @param retryPolicy The {@link RetryPolicy} to use.
	 * @return client An implementation of the {@link CuratorFramework} interface that has been instrumented for participation in a
	 * 	<a href="http://newrelic.com">New Relic</a> transaction trace.
	 * @see CuratorFrameworkFactory#newClient(String, RetryPolicy)
	 * @see <a href="http://newrelic.com">New Relic</a>
	 */
	public static CuratorFramework newClient(final String connectString, final RetryPolicy retryPolicy) {
		return newClient(connectString, DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS, retryPolicy);
	}

	/**
	 * Creates a new client.
	 * @param connectString The list of servers to connect to.
	 * @param sessionTimeoutMs The session timeout in milliseconds.
	 * @param connectionTimeoutMs The connection timeout in milliseconds.
	 * @param retryPolicy The {@link RetryPolicy} to use.
	 * @return client An implementation of the {@link CuratorFramework} interface that has been instrumented for participation in a
	 * 	<a href="http://newrelic.com">New Relic</a> transaction trace.
	 * @see CuratorFrameworkFactory#newClient(String, int, int, RetryPolicy)
	 * @see <a href="http://newrelic.com">New Relic</a>
	 */
	public static CuratorFramework newClient(final String connectString, final int sessionTimeoutMs, final int connectionTimeoutMs, final RetryPolicy retryPolicy) {
		return new NewRelicWrappedCuratorFrameworkImpl(CuratorFrameworkFactory.newClient(connectString, sessionTimeoutMs, connectionTimeoutMs,  retryPolicy));
	}
}
