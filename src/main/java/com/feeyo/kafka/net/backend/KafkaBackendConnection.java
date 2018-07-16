package com.feeyo.kafka.net.backend;

import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.net.backend.BackendConnection;
import com.feeyo.redis.net.front.NetFlowGuard;
import com.feeyo.redis.net.front.RedisFrontConnection;

/**
 * Kafka Connection
 * 
 * @author zhuam
 *
 */
public class KafkaBackendConnection extends BackendConnection {
	
	private static Logger LOGGER = LoggerFactory.getLogger( KafkaBackendConnection.class );

	public KafkaBackendConnection(SocketChannel channel) {
		super(channel);
	}


	@Override
	protected boolean flowGuard(long length) {
		
		if (attachement != null && (attachement instanceof RedisFrontConnection)) {

			RedisFrontConnection frontCon = (RedisFrontConnection) attachement;
			NetFlowGuard netflowGuard = frontCon.getNetFlowGuard();
			if (netflowGuard != null && netflowGuard.consumeBytes(frontCon.getPassword(), length)) {
				LOGGER.warn("##flow clean##,  kafkaBackend: {} ", this);
				this.close(" netflow problem, response clean. ");
				return true;
			}
		}
		return false;
	}
	
}
