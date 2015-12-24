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

package org.springframework.cloud.etcd.endpoint;

import mousio.etcd4j.EtcdClient;
import mousio.etcd4j.responses.EtcdException;
import mousio.etcd4j.responses.EtcdKeysResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @author Jordan Demeulenaere
 */
@RestController
@RequestMapping("etcd")
public class EtcdRestEndpoint {

	public static final Logger log = LoggerFactory.getLogger(EtcdRestEndpoint.class);
	@Autowired
	private EtcdClient etcdClient;

	@RequestMapping(value = "dump", method = RequestMethod.GET, produces = "application/json")
	public Map<String, String> dump() {
		Map<String, String> values = new HashMap<>();
		try {
			EtcdKeysResponse response = etcdClient.getAll().recursive().send().get();
			if (response.node != null && response.node.nodes != null) {
				for (EtcdKeysResponse.EtcdNode node : response.node.nodes) {
					process(node, values);
				}
			}
		} catch (IOException|TimeoutException|EtcdException e) {
			log.info("Unable to retrieve the keys");
		}
		return values;
	}

	private void process(EtcdKeysResponse.EtcdNode node, Map<String, String> values) {
		if (!StringUtils.isEmpty(node.value)) {
			values.put(node.key, node.value);
		}

		if (!CollectionUtils.isEmpty(node.nodes)) {
			for (EtcdKeysResponse.EtcdNode child : node.nodes) {
				process(child, values);
			}
		}
	}

	@RequestMapping(value = "push", method = RequestMethod.POST)
	public ResponseEntity<String> push(@RequestBody Map<String, String> values) {
		for (Map.Entry<String, String> entry : values.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();
			try {
				etcdClient.put(key, value).send().get();
			} catch (IOException|TimeoutException|EtcdException e) {
				log.info(String.format("Unable to send set request for (%s, %s)", key, value));
				return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
			}
		}
		return new ResponseEntity<>(HttpStatus.OK);
	}

	@RequestMapping(value = "rm", method = RequestMethod.POST)
	public ResponseEntity<String> delete(@RequestBody List<String> keys) {
		for (String key : keys) {
			try {
				etcdClient.delete(key).send().get();
			} catch (IOException|TimeoutException|EtcdException e) {
				// Skip key not found exceptions
				if (!(e instanceof EtcdException) || ((EtcdException) e).errorCode != 100 ) {
					log.info(String.format("Unable to send delete request for %s", key));
					return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
				}
			}
		}
		return new ResponseEntity<>(HttpStatus.OK);
	}

	@RequestMapping(value = "rmDir", method = RequestMethod.POST)
	public ResponseEntity<String> deleteDir(@RequestBody List<String> keys) {
		for (String key : keys) {
			try {
				etcdClient.deleteDir(key).recursive().send().get();
			} catch (IOException|TimeoutException|EtcdException e) {
				// Skip key not found exceptions
				if (!(e instanceof EtcdException) || ((EtcdException) e).errorCode != 100 ) {
					log.info(String.format("Unable to send delete request for %s", key));
					return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
				}
			}
		}
		return new ResponseEntity<>(HttpStatus.OK);
	}


}
