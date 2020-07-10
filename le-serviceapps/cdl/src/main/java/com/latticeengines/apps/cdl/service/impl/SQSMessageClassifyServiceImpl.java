package com.latticeengines.apps.cdl.service.impl;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.S3ImportService;
import com.latticeengines.apps.cdl.service.SQSMessageClassifyService;

@Component("sqsMessageClassifyService")
public class SQSMessageClassifyServiceImpl implements SQSMessageClassifyService {

    private static final Logger log = LoggerFactory.getLogger(SQSMessageClassifyServiceImpl.class);

    @Inject
    private S3ImportService s3ImportService;

    @Value("${cdl.s3.import.enabled}")
    private boolean importEnabled;

    @PostConstruct
    public void initialize() {
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(//
                updateMessageUrlRunnable(), 2, //
                30, TimeUnit.SECONDS);
    }

    private Runnable updateMessageUrlRunnable() {
        return () -> {
            if (importEnabled) {
                log.info("Import enabled for current stack, start classify import message!");
                s3ImportService.updateMessageUrl();
            }
        };
    }
}
