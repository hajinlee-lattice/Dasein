package com.latticeengines.dataplatform.service.impl;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.yarn.container.YarnContainer;

/**
 * Container which just sleeps.
 * 
 * @author Feng Meng
 * 
 */
public class KillApplicationContainer implements YarnContainer {

	private static final Log log = LogFactory.getLog(KillApplicationContainer.class);

	@Override
	public void run() {
		log.info("Hello from KillApplicationContainer, gonna sleep next 2 minutes...");

		for (int i = 0; i < 24; i++) {
			log.info("Waiting to get killed");
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
			}
		}
		log.info("Hello from KillApplicationContainer; it seems I wasn't killed.");
	}

	@Override
	public void setEnvironment(Map<String, String> environment) {
	}

	@Override
	public void setParameters(Properties parameters) {
	}

}
