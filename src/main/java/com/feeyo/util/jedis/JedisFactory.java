package com.feeyo.util.jedis;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import com.feeyo.util.jedis.exception.JedisException;

/**
 * PoolableObjectFactory custom impl.
 */
class JedisFactory implements PooledObjectFactory<JedisConnection> {
	//
	private final AtomicReference<HostAndPort> hostAndPort = new AtomicReference<HostAndPort>();
	private final int connectionTimeout;
	private final int soTimeout;
	private final String password;

	public JedisFactory(final String host, final int port, final int connectionTimeout, final int soTimeout,
			final String password) {
		this.hostAndPort.set(new HostAndPort(host, port));
		this.connectionTimeout = connectionTimeout;
		this.soTimeout = soTimeout;
		this.password = password;
	}

	public void setHostAndPort(final HostAndPort hostAndPort) {
		this.hostAndPort.set(hostAndPort);
	}

	@Override
	public void activateObject(PooledObject<JedisConnection> pooledJedis) throws Exception {
	}

	@Override
	public void destroyObject(PooledObject<JedisConnection> pooledJedis) throws Exception {
		final JedisConnection jedis = pooledJedis.getObject();
		if (jedis.isConnected()) {
			try {
				try {
					jedis.sendCommand(RedisCommand.QUIT);
					jedis.getStatusCodeReply();
				} catch (Exception e) {
				}
				jedis.disconnect();
			} catch (Exception e) {

			}
		}

	}

	@Override
	public PooledObject<JedisConnection> makeObject() throws Exception {
		final HostAndPort hostAndPort = this.hostAndPort.get();

		final JedisConnection jedis = new JedisConnection(hostAndPort.getHost(), hostAndPort.getPort(),
				connectionTimeout, soTimeout);

		try {
			jedis.connect();
			if (password != null) {
				jedis.sendCommand(RedisCommand.AUTH, password);
				jedis.getStatusCodeReply();
			}
		} catch (JedisException je) {
			jedis.close();
			throw je;
		}

		return new DefaultPooledObject<JedisConnection>(jedis);

	}

	@Override
	public void passivateObject(PooledObject<JedisConnection> pooledJedis) throws Exception {
		// TODO maybe should select db 0? Not sure right now.
	}

	@Override
	public boolean validateObject(PooledObject<JedisConnection> pooledJedis) {
		final JedisConnection jedis = pooledJedis.getObject();
		try {
			HostAndPort hostAndPort = this.hostAndPort.get();

			String connectionHost = jedis.getHost();
			int connectionPort = jedis.getPort();

			return hostAndPort.getHost().equals(connectionHost) && hostAndPort.getPort() == connectionPort
					&& jedis.isConnected() && jedis.ping().equals("PONG");
		} catch (final Exception e) {
			return false;
		}
	}
}