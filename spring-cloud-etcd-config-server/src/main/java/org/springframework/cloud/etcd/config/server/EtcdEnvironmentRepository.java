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

package org.springframework.cloud.etcd.config.server;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.config.environment.Environment;
import org.springframework.cloud.config.environment.PropertySource;
import org.springframework.cloud.config.server.environment.EnvironmentRepository;
import org.springframework.cloud.config.server.environment.SearchPathLocator;
import org.springframework.cloud.etcd.config.EtcdPropertySource;
import org.springframework.cloud.etcd.config.EtcdPropertySourceLocator;
import org.springframework.util.StringUtils;

import java.util.List;

/**
 * @author Jordan Demeulenaere
 */
public class EtcdEnvironmentRepository
		implements EnvironmentRepository, SearchPathLocator {

	@Autowired
	private EtcdPropertySourceLocator etcdPropertySourceLocator;

	@Override
	public Environment findOne(String application, String profiles, String label) {
		String[] profilesArray = StringUtils.commaDelimitedListToStringArray(profiles);
		List<EtcdPropertySource> propertySources = etcdPropertySourceLocator.locateEtcdPropertySources(application,
				profilesArray);
		Environment environment = new Environment(application, profilesArray, label, null);
		for (EtcdPropertySource etcdPropertySources : propertySources) {
			PropertySource propertySource = new PropertySource(etcdPropertySources.getName(),
					etcdPropertySources.getPropertiesMap());
			environment.add(propertySource);
		}
		return environment;
	}

	@Override
	public Locations getLocations(String application, String profile, String label) {
		return null;
	}

}
