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
package org.apache.curator.newrelic.framework.imps;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.NewRelicWrappedCuratorZookeeperClient;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.api.GetACLBuilder;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.api.SetACLBuilder;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.curator.framework.api.SyncBuilder;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.EnsurePath;

import com.newrelic.api.agent.Trace;

/**
 * Wrapped {@link CuratorFramework} implementation that enables participation
 * in a new <a href="http://newrelic.com">New Relic</a> transaction trace.
 *
 * @author Jonathan Pearlin
 * @since 1.0.0
 * @see CuratorFramework
 * @see <a href="http://newrelic.com">New Relic</a>
 */
public class NewRelicWrappedCuratorFrameworkImpl implements CuratorFramework {

	/**
	 * Delegated {@link CuratorFramework} that will be used to communicate
	 * with a ZooKeeper server/ensemble.
	 */
	private final CuratorFramework delegate;

	/**
	 * Constructs a new {@link NewRelicWrappedCuratorFrameworkImpl} instance that
	 * defers to the provided delegate {@link CuratorFramework}.
	 * @param curatorFramework The delegate {@link CuratorFramework} (may not be {@code null}).
	 * @throws IllegalArgumentException if the provided delegate {@link RetryPolicy}
	 * 	is {@code null}.
	 */
	public NewRelicWrappedCuratorFrameworkImpl(final CuratorFramework curatorFramework) {
		if(curatorFramework == null) {
			throw new IllegalArgumentException("Curator framework delegate may not be null.");
		}

		delegate = curatorFramework;
	}

	@Override
	@Trace(dispatcher=true)
	public void start() {
		delegate.start();
	}

	@Override
	@Trace(dispatcher=true)
	public void close() {
		delegate.close();
	}

	@Override
	@Trace(dispatcher=true)
	public CuratorFrameworkState getState() {
		return delegate.getState();
	}

	@SuppressWarnings("deprecation")
	@Override
	@Trace(dispatcher=true)
	public boolean isStarted() {
		return delegate.isStarted();
	}

	@Override
	@Trace(dispatcher=true)
	public CreateBuilder create() {
		return delegate.create();
	}

	@Override
	@Trace(dispatcher=true)
	public DeleteBuilder delete() {
		return delegate.delete();
	}

	@Override
	@Trace(dispatcher=true)
	public ExistsBuilder checkExists() {
		return delegate.checkExists();
	}

	@Override
	@Trace(dispatcher=true)
	public GetDataBuilder getData() {
		return delegate.getData();
	}

	@Override
	@Trace(dispatcher=true)
	public SetDataBuilder setData() {
		return delegate.setData();
	}

	@Override
	@Trace(dispatcher=true)
	public GetChildrenBuilder getChildren() {
		return delegate.getChildren();
	}

	@Override
	@Trace(dispatcher=true)
	public GetACLBuilder getACL() {
		return delegate.getACL();
	}

	@Override
	@Trace(dispatcher=true)
	public SetACLBuilder setACL() {
		return delegate.setACL();
	}

	@Override
	@Trace(dispatcher=true)
	public CuratorTransaction inTransaction() {
		return delegate.inTransaction();
	}

	@SuppressWarnings("deprecation")
	@Override
	@Trace(dispatcher=true)
	public void sync(final String path, final Object backgroundContextObject) {
		delegate.sync(path, backgroundContextObject);
	}

	@Override
	@Trace(dispatcher=true)
	public SyncBuilder sync() {
		return delegate.sync();
	}

	@Override
	@Trace(dispatcher=true)
	public Listenable<ConnectionStateListener> getConnectionStateListenable() {
		return delegate.getConnectionStateListenable();
	}

	@Override
	@Trace(dispatcher=true)
	public Listenable<CuratorListener> getCuratorListenable() {
		return delegate.getCuratorListenable();
	}

	@Override
	@Trace(dispatcher=true)
	public Listenable<UnhandledErrorListener> getUnhandledErrorListenable() {
		return delegate.getUnhandledErrorListenable();
	}

	@SuppressWarnings("deprecation")
	@Override
	@Trace(dispatcher=true)
	public CuratorFramework nonNamespaceView() {
		return delegate.nonNamespaceView();
	}

	@Override
	@Trace(dispatcher=true)
	public CuratorFramework usingNamespace(final String newNamespace) {
		return delegate.usingNamespace(newNamespace);
	}

	@Override
	@Trace(dispatcher=true)
	public String getNamespace() {
		return delegate.getNamespace();
	}

	@Override
	@Trace(dispatcher=true)
	public CuratorZookeeperClient getZookeeperClient() {
		try {
			return new NewRelicWrappedCuratorZookeeperClient(delegate.getZookeeperClient());
		} catch(final Exception e) {
			return delegate.getZookeeperClient();
		}
	}

	@Override
	@Trace(dispatcher=true)
	public EnsurePath newNamespaceAwareEnsurePath(final String path) {
		return delegate.newNamespaceAwareEnsurePath(path);
	}
}