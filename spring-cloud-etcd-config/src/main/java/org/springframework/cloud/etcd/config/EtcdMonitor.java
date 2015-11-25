package org.springframework.cloud.etcd.config;

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
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.UUID;

public class EtcdMonitor
		implements ApplicationEventPublisherAware,
		ApplicationContextAware,
		ResponsePromise.IsSimplePromiseResponseHandler<EtcdKeysResponse> {

	public static final Logger log = LoggerFactory.getLogger(EtcdMonitor.class);

	private ApplicationEventPublisher applicationEventPublisher;
	private String contextId = UUID.randomUUID().toString();

	private long lastModifiedIndex = 0;
	@Autowired
	private EtcdClient etcdClient;
	@Autowired
	private EtcdConfigProperties etcdConfigProperties;

	private final RetryPolicy retryPolicy = new RetryWithExponentialBackOff(20, -1, -1);

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
				/* Send refresh to everyone */
//				refreshEveryone();
				syncWithEtcd();
			}
		} catch (Exception e) {
			log.warn("Unable to init property source: " + prefix, e);
		}
	}

	public void refresh(String destination) {
		log.info("Refresh for : " + destination);
		applicationEventPublisher.publishEvent(
				new RefreshRemoteApplicationEvent(this, this.contextId, destination));
	}

//	@EventListener
//	public void handleRefreshRemoteApplicationEvent(RefreshRemoteApplicationEvent event) {
//		log.info("Listened to refreshRemoteApplicationEvent");
//	}

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
	public void onResponse(ResponsePromise<EtcdKeysResponse> responsePromise) {
		try {
			EtcdKeysResponse response = responsePromise.get();
			if (response.node != null) {
				process(response.node);

				String key = response.node.key;
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
					refresh(service);
				} else {
					log.info("Could not extract context from key: " + key);
				}
			}
		} catch (Exception e) {
			log.warn("Unable to extract etcd response");
		}
		syncWithEtcd();
	}

}
