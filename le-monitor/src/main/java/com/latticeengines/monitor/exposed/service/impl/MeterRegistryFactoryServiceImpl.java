package com.latticeengines.monitor.exposed.service.impl;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.monitor.exposed.service.MeterRegistryFactoryService;

import io.micrometer.core.instrument.MeterRegistry;

@Lazy
@Component("meterRegistryFactory")
public class MeterRegistryFactoryServiceImpl implements MeterRegistryFactoryService {

    private static final Logger log = LoggerFactory.getLogger(MeterRegistryFactoryServiceImpl.class);

    @Resource(name = "rootRegistry")
    private MeterRegistry rootRegistry;

    @Resource(name = "rootHostRegistry")
    private MeterRegistry rootHostRegistry;

    @Override
    public MeterRegistry getServiceLevelRegistry() {
        return rootRegistry;
    }

    @Override
    public MeterRegistry getHostLevelRegistry() {
        return rootHostRegistry;
    }

    /*
     * close root registries automatically
     */
    @PreDestroy
    private void cleanup() {
        log.info("Closing all meter registries");
        rootHostRegistry.close();
        rootRegistry.close();
        log.info("All meter registries closed");
    }
}
