package org.springframework.cloud.etcd.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EtcdMonitorAutoConfiguration {

	@Bean
	public EtcdMonitor etcdMonitor() {
		return new EtcdMonitor();
	}

}
