package com.latticeengines.apps.cdl.service.impl;

import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.S3ImportService;
import com.latticeengines.apps.cdl.service.SQSMessageClassifyService;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;

@Component("sqsMessageClassifyService")
public class SQSMessageClassifyServiceImpl implements SQSMessageClassifyService {

    private static final Logger log = LoggerFactory.getLogger(SQSMessageClassifyServiceImpl.class);

    @Inject
    private S3ImportService s3ImportService;

    @Value("${cdl.s3.import.enabled}")
    private boolean importEnabled;

    @Value("${cdl.s3.import.message.scan.period.seconds}")
    private int scanPeriod;

    @PostConstruct
    public void initialize() {
        if (importEnabled) {
            log.info(String.format("Import enabled for current stack, create scheduled task (scan period %d seconds) " +
                    "for import message classification.", scanPeriod));
            ThreadPoolUtils.getScheduledThreadPool("sqs-message-classification", 1).scheduleAtFixedRate(//
                    updateMessageUrlRunnable(), 2, scanPeriod, TimeUnit.SECONDS);
        }
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
