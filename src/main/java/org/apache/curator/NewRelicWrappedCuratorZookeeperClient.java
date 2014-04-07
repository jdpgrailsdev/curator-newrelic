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
package org.apache.curator;

import java.lang.reflect.Field;
import java.util.Queue;

import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.newrelic.NewRelicWrappedZookeeper;

/**
 * Wrapped version of the {@link CuratorZookeeperClient} that produces a
 * wrapped version of the underlying {@link ZooKeeper} instance.  The
 * {@link ZooKeeper} instance is instrumented to take part in <a href="http://newrelic.com">New Relic</a>
 * transaction traces.
 *
 * @author Jonathan Pearlin
 * @since 1.0.0
 * @see CuratorZookeeperClient
 * @see <a href="http://newrelic.com">New Relic</a>
 */
public class NewRelicWrappedCuratorZookeeperClient extends CuratorZookeeperClient {

	/**
	 * Constructs a new, wrapped {@link CuratorZookeeperClient} that takes part
	 * in <a href="http://newrelic.com">New Relic</a> transaction traces.
	 * @param delegate The delegate {@link CuratorZookeeperClient} from which this
	 * 	wrapped version will be created.
	 * @throws NoSuchFieldException if unable to clone data from the delegate.
	 * @throws SecurityException if unable to clone data from the delegate.
	 * @throws IllegalArgumentException if unable to clone data from the delegate.
	 * @throws IllegalAccessException if unable to clone data from the delegate.
	 */
	public NewRelicWrappedCuratorZookeeperClient(final CuratorZookeeperClient delegate) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		super(getZookeeperFactoryFromDelegate(delegate), getEnsembleProviderFromDelegate(delegate), getSessionTimeoutMsFromDelegate(delegate),
				delegate.getConnectionTimeoutMs(), getWatcherFromDelegate(delegate), delegate.getRetryPolicy(), getCanBeReadOnlyFromDelegate(delegate));
		// Close the delegate so that we don't have duplicate open connections.
		delegate.close();
	}

	/**
	 * Creates a new, wrapped {@link CuratorZookeeperClient} that takes part in <a href="http://newrelic.com">New Relic</a> transaction traces.
	 * @param ensembleProvider The {@link EnsembleProvider}.
	 * @param sessionTimeoutMs The session timeout in milliseconds.  Should be less
	 * 	than the connection timeout.
	 * @param connectionTimeoutMs The connection timeout in milliseconds.
	 * @param watcher Default {@link Watcher} or {@code null}.
	 * @param retryPolicy The {@link RetryPolicy}.
	 * @see CuratorZookeeperClient#CuratorZookeeperClient(EnsembleProvider, int, int, Watcher, RetryPolicy)
	 */
	public NewRelicWrappedCuratorZookeeperClient(final EnsembleProvider ensembleProvider, final int sessionTimeoutMs, final int connectionTimeoutMs, final Watcher watcher, final RetryPolicy retryPolicy) {
		super(ensembleProvider, sessionTimeoutMs, connectionTimeoutMs, watcher, retryPolicy);
	}

	/**
	 * Creates a new, wrapped {@link CuratorZookeeperClient} that takes part in <a href="http://newrelic.com">New Relic</a> transaction traces.
	 * @param connectString The connection string.
	 * @param sessionTimeoutMs The session timeout in milliseconds.  Should be less
	 * 	than the connection timeout.
	 * @param connectionTimeoutMs The connection timeout in milliseconds.
	 * @param watcher Default {@link Watcher} or {@code null}.
	 * @param retryPolicy The {@link RetryPolicy}.
	 * @see CuratorZookeeperClient#CuratorZookeeperClient(String, int, int, Watcher, RetryPolicy)
	 */
	public NewRelicWrappedCuratorZookeeperClient(final String connectString, final int sessionTimeoutMs, final int connectionTimeoutMs, final Watcher watcher, final RetryPolicy retryPolicy) {
		super(connectString, sessionTimeoutMs, connectionTimeoutMs, watcher, retryPolicy);
	}

	/**
	 * Creates a new, wrapped {@link CuratorZookeeperClient} that takes part in <a href="http://newrelic.com">New Relic</a> transaction traces.
	 * @param zookeeperFactory The factory for creating {@link ZooKeeper} instances.
	 * @param ensembleProvider The {@link EnsembleProvider}.
	 * @param sessionTimeoutMs The session timeout in milliseconds.  Should be less
	 * 	than the connection timeout.
	 * @param connectionTimeoutMs The connection timeout in milliseconds.
	 * @param watcher Default {@link Watcher} or {@code null}.
	 * @param retryPolicy The {@link RetryPolicy}.
	 * @param canBeReadOnly {@code true} to allow the ZooKeeper client to enter read only mode in case of a network partition. See
	 *  {@link ZooKeeper#ZooKeeper(String, int, Watcher, long, byte[], boolean)} for details.
	 *  @see CuratorZookeeperClient#CuratorZookeeperClient(ZookeeperFactory, EnsembleProvider, int, int, Watcher, RetryPolicy, boolean)
	 */
	public NewRelicWrappedCuratorZookeeperClient(final ZookeeperFactory zookeeperFactory, final EnsembleProvider ensembleProvider, final int sessionTimeoutMs, final int connectionTimeoutMs, final Watcher watcher, final RetryPolicy retryPolicy, final boolean canBeReadOnly) {
		super(zookeeperFactory, ensembleProvider, sessionTimeoutMs, connectionTimeoutMs, watcher, retryPolicy, canBeReadOnly);
	}

	@Override
	public ZooKeeper getZooKeeper() throws Exception {
		return new NewRelicWrappedZookeeper(super.getZooKeeper(), getCurrentConnectionString());
	}

	/**
	 * Retrieves the session timeout from the delegate {@link CuratorZookeeperClient} using
	 * reflection.  This is because the fields that need to be cloned are not accessible outside
	 * of the {@link CuratorZookeeperClient} instance.
	 * @param delegate The delegate {@link CuratorZookeeperClient} that is being cloned.
	 * @return The session timeout value extracted from the {@link CuratorZookeeperClient}.
	 * @throws NoSuchFieldException if unable to extract the session timeout from the delegate.
	 * @throws SecurityException if unable to extract the session timeout from the delegate.
	 * @throws IllegalArgumentException if unable to extract the session timeout from the delegate.
	 * @throws IllegalAccessException if unable to extract the session timeout from the delegate.
	 */
	private static int getSessionTimeoutMsFromDelegate(final CuratorZookeeperClient delegate) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		final ConnectionState state = getConnectionStateFromDelegate(delegate);
		final Field sessionTimeoutMs = ConnectionState.class.getDeclaredField("sessionTimeoutMs");
		sessionTimeoutMs.setAccessible(true);
		return (int)sessionTimeoutMs.get(state);
	}

	/**
	 * Retrieves the {@link EnsembleProvider} from the delegate {@link CuratorZookeeperClient} using
	 * reflection.  This is because the fields that need to be cloned are not accessible outside of
	 * the {@link CuratorZookeeperClient} instance.
	 * @param delegate The delegate {@link CuratorZookeeperClient} that is being cloned.
	 * @return The {@link EnsembleProvider} extracted from the {@link CuratorZookeeperClient}.
	 * @throws NoSuchFieldException if unable to extract the ensemble provider from the delegate.
	 * @throws SecurityException if unable to extract the ensemble provider from the delegate.
	 * @throws IllegalArgumentException if unable to extract the ensemble provider from the delegate.
	 * @throws IllegalAccessException if unable to extract the ensemble provider from the delegate.
	 */
	private static EnsembleProvider getEnsembleProviderFromDelegate(final CuratorZookeeperClient delegate) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		final ConnectionState state = getConnectionStateFromDelegate(delegate);
		final Field ensembleProvider = ConnectionState.class.getDeclaredField("ensembleProvider");
		ensembleProvider.setAccessible(true);
		return (EnsembleProvider)ensembleProvider.get(state);
	}

	/**
	 * Retrieves the {@link Watcher} from the delegate {@link CuratorZookeeperClient} using
	 * reflection.  This is because the fields that need to be cloned are not accessible outside
	 * of the {@link CuratorZookeeperClient} instance.
	 * @param delegate The delegate {@link CuratorZookeeperClient} that is being cloned.
	 * @return The {@link Watcher} extracted from the {@link CuratorZookeeperClient}.
	 * @throws NoSuchFieldException if unable to extract the watcher from the delegate.
	 * @throws SecurityException if unable to extract the watcher from the delegate.
	 * @throws IllegalArgumentException if unable to extract the watcher from the delegate.
	 * @throws IllegalAccessException if unable to extract the watcher from the delegate.
	 */
	@SuppressWarnings("unchecked")
	private static Watcher getWatcherFromDelegate(final CuratorZookeeperClient delegate) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		final ConnectionState state = getConnectionStateFromDelegate(delegate);
		final Field parentWatchers = ConnectionState.class.getDeclaredField("parentWatchers");
		parentWatchers.setAccessible(true);
		return ((Queue<Watcher>)parentWatchers.get(state)).peek();
	}

	/**
	 * Retrieves the {@link ZookeeperFactory} from the delegate {@link CuratorZookeeperClient} using
	 * reflection.  This is because the fields that need to be cloned are not accessible outside of
	 * the {@link CuratorZookeeperClient} instance.
	 * @param delegate The delegate {@link CuratorZookeeperClient} that is being cloned.
	 * @return The {@link ZookeeperFactory} extracted from the {@link CuratorZookeeperClient}.
	 * @throws NoSuchFieldException if unable to extract the ZooKeeper factory from the delegate.
	 * @throws SecurityException if unable to extract the ZooKeeper factory from the delegate.
	 * @throws IllegalArgumentException if unable to extract the ZooKeeper factory from the delegate.
	 * @throws IllegalAccessException if unable to extract the ZooKeeper factory from the delegate.
	 */
	private static ZookeeperFactory getZookeeperFactoryFromDelegate(final CuratorZookeeperClient delegate) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		final HandleHolder holder = getHandleHolderFromDelegate(delegate);
		final Field zookeeperFactory = HandleHolder.class.getDeclaredField("zookeeperFactory");
		zookeeperFactory.setAccessible(true);
		return (ZookeeperFactory)zookeeperFactory.get(holder);
	}

	/**
	 * Retrieves the read only status from the delegate {@link CuratorZookeeperClient} using
	 * reflection.  This is because the fields that need to be cloned are not accessible outside of
	 * the {@link CuratorZookeeperClient} instance.
	 * @param delegate The delegate {@link CuratorZookeeperClient} that is being cloned.
	 * @return The read only status flag extracted from the {@link CuratorZookeeperClient}.
	 * @throws NoSuchFieldException if unable to extract the read only flag from the delegate.
	 * @throws SecurityException if unable to extract the read only flag from the delegate.
	 * @throws IllegalArgumentException if unable to extract the read only flag from the delegate.
	 * @throws IllegalAccessException if unable to extract the read only flag from the delegate.
	 */
	private static boolean getCanBeReadOnlyFromDelegate(final CuratorZookeeperClient delegate) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		final HandleHolder holder = getHandleHolderFromDelegate(delegate);
		final Field canBeReadOnly = HandleHolder.class.getDeclaredField("canBeReadOnly");
		canBeReadOnly.setAccessible(true);
		return (Boolean)canBeReadOnly.get(holder);
	}

	/**
	 * Retrieves the {@link ConnectionState} instance from the delegate {@link CuratorZookeeperClient} using
	 * reflection.  This is because the fields that need to be cloned are not accessible outside of the
	 * {@link CuratorZookeeperClient} instance.
	 * @param delegate The delegate {@link CuratorZookeeperClient} that is being cloned.
	 * @return The {@link ConnectionState} instance extracted from the {@link CuratorZookeeperClient}.
	 * @throws NoSuchFieldException if unable to extract the connection state from the delegate.
	 * @throws SecurityException if unable to extract the connection state from the delegate.
	 * @throws IllegalArgumentException if unable to extract the connection state from the delegate.
	 * @throws IllegalAccessException if unable to extract the connection state from the delegate.
	 */
	private static ConnectionState getConnectionStateFromDelegate(final CuratorZookeeperClient delegate) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		final Field state = CuratorZookeeperClient.class.getDeclaredField("state");
		state.setAccessible(true);
		return (ConnectionState)state.get(delegate);
	}

	/**
	 * Retrieves the {@link HandleHolder} from the delegate {@link CuratorZookeeperClient} using
	 * reflection.  This is because the fields that need to be cloned are not accessible outside of the
	 * {@link CuratorZookeeperClient} instance.
	 * @param delegate The delegate {@link CuratorZookeeperClient} that is being cloned.
	 * @return The {@link HandleHolder} instance extracted from the {@link CuratorZookeeperClient}.
	 * @throws NoSuchFieldException if unable to extract the handle holder from the delegate.
	 * @throws SecurityException if unable to extract the handle holder from the delegate.
	 * @throws IllegalArgumentException if unable to extract the handle holder from the delegate.
	 * @throws IllegalAccessException if unable to extract the handle holder from the delegate.
	 */
	private static HandleHolder getHandleHolderFromDelegate(final CuratorZookeeperClient delegate) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		final ConnectionState state = getConnectionStateFromDelegate(delegate);
		final Field zooKeeper = ConnectionState.class.getDeclaredField("zooKeeper");
		zooKeeper.setAccessible(true);
		return (HandleHolder)zooKeeper.get(state);
	}
}
