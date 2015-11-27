/*
 * Copyright 2013-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.etcd.monitor;

import mousio.client.promises.ResponsePromise;
import mousio.client.retry.RetryPolicy;
import mousio.client.retry.RetryWithExponentialBackOff;
import mousio.etcd4j.EtcdClient;
import mousio.etcd4j.responses.EtcdKeysResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.bus.event.RefreshRemoteApplicationEvent;
import org.springframework.cloud.etcd.config.EtcdConfigProperties;
import org.springframework.cloud.etcd.config.EtcdConstants;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.*;

/**
 * @author Jordan Demeulenaere
 */
public class EtcdMonitor
		implements ApplicationEventPublisherAware,
		ApplicationContextAware,
		ResponsePromise.IsSimplePromiseResponseHandler<EtcdKeysResponse> {

	public static final Logger log = LoggerFactory.getLogger(EtcdMonitor.class);
	public static final int successiveRequestsTimeoutMs = 1000;

	private ApplicationEventPublisher applicationEventPublisher;
	private String contextId = UUID.randomUUID().toString();

	private long lastModifiedIndex = 0;
	private long lastRefreshedIndex = 0;

	@Autowired
	private EtcdClient etcdClient;
	@Autowired
	private EtcdConfigProperties etcdConfigProperties;

	private final RetryPolicy retryPolicy = new RetryWithExponentialBackOff(20, -1, -1);

	private Timer refreshTimer;
	private final Set<String> servicesToRefresh;
	private TimerTask refreshTask;

	public EtcdMonitor() {
		servicesToRefresh = new HashSet<>();
		refreshTimer = new Timer("EtcdRefresh", true);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.contextId = applicationContext.getId();
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	@PostConstruct
	public void initLastModifiedIndex() {
		String prefix = etcdConfigProperties.getPrefix();
		try {
			EtcdKeysResponse response = etcdClient.getDir(prefix)
					.recursive()
					.setRetryPolicy(retryPolicy)
					.send()
					.get();
			if (response.node != null) {
				process(response.node);
				syncWithEtcd();
			} else {
				log.warn("Unable to fetch configuration directory : " + prefix);
			}
		} catch (Exception e) {
			log.warn("Unable to fetch configuration directory : " + prefix);
		}
	}

	private void process(EtcdKeysResponse.EtcdNode node) {
		lastModifiedIndex = Math.max(node.modifiedIndex, lastModifiedIndex);

		if (!CollectionUtils.isEmpty(node.nodes)) {
			for (EtcdKeysResponse.EtcdNode child : node.nodes) {
				process(child);
			}
		}
	}

	public void syncWithEtcd() {
		String prefix = etcdConfigProperties.getPrefix();
		try {
			etcdClient.getDir(prefix)
					.recursive()
					.waitForChange(lastModifiedIndex + 1)
					.setRetryPolicy(retryPolicy)
					.send()
					.addListener(this);
		} catch (Exception e) {
			log.warn("Unable to init property source: " + prefix, e);
		}
	}

	@Override
	public synchronized void onResponse(ResponsePromise<EtcdKeysResponse> responsePromise) {
		try {
			EtcdKeysResponse response = responsePromise.get();
			if (response.node != null) {
				process(response.node); // update lastModifiedIndex

				String key = response.node.key;
				/* In case multiple keys are modified at the same time, we perform another request) */
				String service = serviceForKey(key);
				if (service != null) {
					servicesToRefresh.add(service);
				}
				if (refreshTask != null) {
					refreshTask.cancel();
				}
				refreshTask = new TimerTask() {
					@Override
					public void run() {
						refreshServices();
					}
				};
				refreshTimer.schedule(refreshTask, successiveRequestsTimeoutMs);
			} else {
				log.warn("Unable to extract etcd response");
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.warn("Unable to extract etcd response");
		}
		syncWithEtcd();
	}

	private synchronized void refreshServices() {
		log.info("Refreshing services");
		if (lastRefreshedIndex < lastModifiedIndex) {
			if (servicesToRefresh.contains("*") ||
					servicesToRefresh.contains(etcdConfigProperties.getDefaultContext())) {
				refresh("*");
			} else {
				for (String service : servicesToRefresh) {
					refresh(service);
				}
			}
			servicesToRefresh.clear();
			lastRefreshedIndex = lastModifiedIndex;
		} else {
			log.warn("No need to refresh");
		}
	}

	public void refresh(String destination) {
		log.info("Refresh for : " + destination);
		applicationEventPublisher.publishEvent(
				new RefreshRemoteApplicationEvent(this, this.contextId, destination));
	}

	public String serviceForKey(String key) {
		if (key.startsWith(EtcdConstants.PATH_SEPARATOR)) {
			key = key.substring(1);
		}

		String[] keyArray = key.split(EtcdConstants.PATH_SEPARATOR);
		if (keyArray.length >= 2) {
			/* Refreshing the service */
			String serviceWithProfiles = keyArray[1];
			String[] serviceWithProfilesArray = serviceWithProfiles
					.split(etcdConfigProperties.getProfileSeparator());
			String service = serviceWithProfilesArray[0];
			return service;
		} else if (keyArray.length == 1 && etcdConfigProperties.getPrefix().equals(keyArray[0])) {
			/* prefix directory has changed, refresh everyone */
			return "*";
		} else {
			log.info("Unable to extract context from key: " + key);
			return null;
		}
	}

}
