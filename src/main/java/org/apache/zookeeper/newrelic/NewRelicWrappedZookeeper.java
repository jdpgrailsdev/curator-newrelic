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
package org.apache.zookeeper.newrelic;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

import org.apache.zookeeper.AsyncCallback.ACLCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.newrelic.api.agent.Trace;

/**
 * Wrapped version of the {@link ZooKeeper} client.  The {@link ZooKeeper} instance
 * is instrumented to take part in <a href="http://newrelic.com">New Relic</a> transaction traces.
 *
 * @author Jonathan Pearlin
 * @since 1.0.0
 * @see ZooKeeper
 * @see <a href="http://newrelic.com">New Relic</a>
 */
public class NewRelicWrappedZookeeper extends ZooKeeper {

	/**
	 * Creates a new, wrapped {@code NewRelicWrappedZookeeper} instance that takes part in
	 * <a href="http://newrelic.com">New Relic</a> transaction traces.
	 * @param delegate The delegate {@link ZooKeeper} instance that will be used to create
	 * 	this wrapped instance.
	 * @param connectionString The ZooKeeper connection string.
	 * @throws NoSuchFieldException if unable to clone the delegate.
	 * @throws SecurityException if unable to clone the delegate.
	 * @throws IllegalArgumentException if unable to clone the delegate.
	 * @throws IllegalAccessException if unable to clone the delegate.
	 * @throws IOException if unable to clone the delegate.
	 * @throws Exception if unable to clone the delegate.
	 */
	public NewRelicWrappedZookeeper(final ZooKeeper delegate, final String connectionString) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, IOException, Exception {
		super(connectionString, delegate.getSessionTimeout(), getWatcherFromDelegate(delegate),
				delegate.getSessionId(), delegate.getSessionPasswd(), getCanBeReadOnlyFromDelegate(delegate));

		// Close the delegate so that we do not have a duplicate connection.
		delegate.close();
	}

	/**
	 * Retrieves the {@link Watcher} instance from the provided delegate {@link ZooKeeper} instance using
	 * reflection.  This is necessary because the fields that are needed to clone the delegate {@link ZooKeeper}
	 * instance is not accessible from outside of the {@link ZooKeeper} class.
	 * @param delegate The delegate {@link ZooKeeper} instance.
	 * @return The {@link Watcher} extracted from the delegate {@link ZooKeeper} instance.
	 * @throws NoSuchFieldException if unable to extract the watcher from the delegate.
	 * @throws SecurityException if unable to extract the watcher from the delegate.
	 * @throws IllegalArgumentException if unable to extract the watcher from the delegate.
	 * @throws IllegalAccessException if unable to extract the watcher from the delegate.
	 */
	private static Watcher getWatcherFromDelegate(final ZooKeeper delegate) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		final Field watchManager = ZooKeeper.class.getDeclaredField("watchManager");
		watchManager.setAccessible(true);
		final Object zkWatchManager = watchManager.get(delegate);
		final Field defaultWatcher = zkWatchManager.getClass().getDeclaredField("defaultWatcher");
		defaultWatcher.setAccessible(true);
		return (Watcher)defaultWatcher.get(zkWatchManager);
	}

	/**
	 * Retrieves the read only boolean flag from the provided delegate {@link ZooKeeper} instance using
	 * reflection.  This is necessary because the fields that are needed to clone the delegate {@link ZooKeeper}
	 * instance is not accessible from outside of the {@link ZooKeeper} class.
	 * @param delegate The delegate {@link ZooKeeper} instance.
	 * @return The read only boolean flag extracted from the delegate {@link ZooKeeper} instance.
	 * @throws NoSuchFieldException if unable to extract the read only flag from the delegate.
	 * @throws SecurityException if unable to extract the read only flag from the delegate.
	 * @throws IllegalArgumentException if unable to extract the read only flag from the delegate.
	 * @throws IllegalAccessException if unable to extract the read only flag from the delegate.
	 */
	private static boolean getCanBeReadOnlyFromDelegate(final ZooKeeper delegate) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		final Field cnxn = ZooKeeper.class.getDeclaredField("cnxn");
		cnxn.setAccessible(true);
		final ClientCnxn clientCnxn = (ClientCnxn)cnxn.get(delegate);
		final Field readOnly = ClientCnxn.class.getDeclaredField("readOnly");
		readOnly.setAccessible(true);
		return (Boolean)readOnly.get(clientCnxn);
	}

	@Override
	@Trace
	public synchronized void close() throws InterruptedException {
		super.close();
	}

	@Override
	@Trace
	public String create(final String path, final byte[] data, final List<ACL> acl, final CreateMode createMode) throws KeeperException, InterruptedException {
		return super.create(path, data, acl, createMode);
	}

	@Override
	@Trace
	public void create(final String path, final byte[] data, final List<ACL> acl, final CreateMode createMode, final StringCallback cb, final Object ctx) {
		super.create(path, data, acl, createMode, cb, ctx);
	}

	@Override
	@Trace
	public void delete(final String path, final int version) throws InterruptedException, KeeperException {
		super.delete(path, version);
	}

	@Override
	@Trace
	public void delete(final String path, final int version, final VoidCallback cb, final Object ctx) {
		super.delete(path, version, cb, ctx);
	}

	@Override
	@Trace
	public Stat exists(final String path, final Watcher watcher) throws KeeperException, InterruptedException {
		return super.exists(path, watcher);
	}

	@Override
	@Trace
	public Stat exists(final String path, final boolean watch) throws KeeperException, InterruptedException {
		return super.exists(path, watch);
	}

	@Override
	@Trace
	public void exists(final String path, final Watcher watcher, final StatCallback cb, final Object ctx) {
		super.exists(path, watcher, cb, ctx);
	}

	@Override
	@Trace
	public void exists(final String path, final boolean watch, final StatCallback cb, final Object ctx) {
		super.exists(path, watch, cb, ctx);
	}

	@Override
	@Trace
	public byte[] getData(final String path, final Watcher watcher, final Stat stat) throws KeeperException, InterruptedException {
		return super.getData(path, watcher, stat);
	}

	@Override
	@Trace
	public byte[] getData(final String path, final boolean watch, final Stat stat) throws KeeperException, InterruptedException {
		return super.getData(path, watch, stat);
	}

	@Override
	@Trace
	public void getData(final String path, final Watcher watcher, final DataCallback cb, final Object ctx) {
		super.getData(path, watcher, cb, ctx);
	}

	@Override
	@Trace
	public void getData(final String path, final boolean watch, final DataCallback cb, final Object ctx) {
		super.getData(path, watch, cb, ctx);
	}

	@Override
	@Trace
	public Stat setData(final String path, final byte[] data, final int version) throws KeeperException, InterruptedException {
		return super.setData(path, data, version);
	}

	@Override
	@Trace
	public void setData(final String path, final byte[] data, final int version, final StatCallback cb, final Object ctx) {
		super.setData(path, data, version, cb, ctx);
	}

	@Override
	@Trace
	public List<ACL> getACL(final String path, final Stat stat) throws KeeperException, InterruptedException {
		return super.getACL(path, stat);
	}

	@Override
	@Trace
	public void getACL(final String path, final Stat stat, final ACLCallback cb, final Object ctx) {
		super.getACL(path, stat, cb, ctx);
	}

	@Override
	@Trace
	public Stat setACL(final String path, final List<ACL> acl, final int version) throws KeeperException, InterruptedException {
		return super.setACL(path, acl, version);
	}

	@Override
	@Trace
	public void setACL(final String path, final List<ACL> acl, final int version, final StatCallback cb, final Object ctx) {
		super.setACL(path, acl, version, cb, ctx);
	}

	@Override
	@Trace
	public List<String> getChildren(final String path, final Watcher watcher) throws KeeperException, InterruptedException {
		return super.getChildren(path, watcher);
	}

	@Override
	@Trace
	public List<String> getChildren(final String path, final boolean watch) throws KeeperException, InterruptedException {
		return super.getChildren(path, watch);
	}

	@Override
	@Trace
	public void getChildren(final String path, final Watcher watcher, final ChildrenCallback cb, final Object ctx) {
		super.getChildren(path, watcher, cb, ctx);
	}

	@Override
	@Trace
	public void getChildren(final String path, final boolean watch, final ChildrenCallback cb, final Object ctx) {
		super.getChildren(path, watch, cb, ctx);
	}

	@Override
	@Trace
	public List<String> getChildren(final String path, final Watcher watcher, final Stat stat) throws KeeperException, InterruptedException {
		return super.getChildren(path, watcher, stat);
	}

	@Override
	@Trace
	public List<String> getChildren(final String path, final boolean watch, final Stat stat) throws KeeperException, InterruptedException {
		return super.getChildren(path, watch, stat);
	}

	@Override
	@Trace
	public void getChildren(final String path, final Watcher watcher, final Children2Callback cb, final Object ctx) {
		super.getChildren(path, watcher, cb, ctx);
	}

	@Override
	@Trace
	public void getChildren(final String path, final boolean watch, final Children2Callback cb, final Object ctx) {
		super.getChildren(path, watch, cb, ctx);
	}
}