package org.springframework.cloud.etcd.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.config.environment.Environment;
import org.springframework.cloud.config.environment.PropertySource;
import org.springframework.cloud.config.server.environment.EnvironmentRepository;
import org.springframework.cloud.config.server.environment.SearchPathLocator;
import org.springframework.util.StringUtils;

import java.util.List;

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
