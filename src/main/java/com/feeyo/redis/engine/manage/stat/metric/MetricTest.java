package com.feeyo.redis.engine.manage.stat.metric;



import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;

public class MetricTest {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(MetricTest.class);
	
	static final MetricRegistry metrics = new MetricRegistry();

	public static void main(String[] args) throws IOException, InterruptedException {
		
		ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics).convertRatesTo(TimeUnit.SECONDS)
				.convertDurationsTo(TimeUnit.MILLISECONDS).build();
		
		metrics.register("jvm.mem", new MemoryUsageGaugeSet());
		metrics.register("jvm.gc", new GarbageCollectorMetricSet() );
		metrics.register("jvm.threads", new ThreadStatesGaugeSet() );
		reporter.start(30, TimeUnit.SECONDS);

		TimeUnit.SECONDS.sleep(500);
	}
}
