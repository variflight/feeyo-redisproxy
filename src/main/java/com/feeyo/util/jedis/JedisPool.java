package com.feeyo.util.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.feeyo.util.jedis.exception.JedisException;

public class JedisPool extends Pool<JedisConnection> {

  public JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
      int timeout, final String password) {
    super(poolConfig, new JedisFactory(host, port, timeout, timeout, password));
  }



  @Override
	public JedisConnection getResource() {
		JedisConnection jedis = super.getResource();
		jedis.setDataSource(this);
		return jedis;
	}

  /**
   * @deprecated starting from Jedis 3.0 this method will not be exposed. Resource cleanup should be
   *             done using @see {@link redis.clients.jedis.Jedis#close()}
   */
  @Override
  @Deprecated
  public void returnBrokenResource(final JedisConnection resource) {
    if (resource != null) {
      returnBrokenResourceObject(resource);
    }
  }

  /**
   * @deprecated starting from Jedis 3.0 this method will not be exposed. Resource cleanup should be
   *             done using @see {@link redis.clients.jedis.Jedis#close()}
   */
  @Override
  @Deprecated
  public void returnResource(final JedisConnection resource) {
    if (resource != null) {
      try {
        returnResourceObject(resource);
      } catch (Exception e) {
        returnBrokenResource(resource);
        throw new JedisException("Could not return the resource to the pool", e);
      }
    }
  }
}
