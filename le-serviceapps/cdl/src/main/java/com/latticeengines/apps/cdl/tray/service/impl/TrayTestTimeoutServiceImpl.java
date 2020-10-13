package com.latticeengines.apps.cdl.tray.service.impl;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.tray.service.TrayConnectorTestService;
import com.latticeengines.apps.cdl.tray.service.TrayTestTimeoutService;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTest;

@Component("trayTestTimeoutService")
public class TrayTestTimeoutServiceImpl implements TrayTestTimeoutService {
    private static final Logger log = LoggerFactory.getLogger(TrayTestTimeoutServiceImpl.class);

    @Inject
    private TrayConnectorTestService trayConnectorTestService;

    public TrayTestTimeoutServiceImpl() {

    }

    @Override
    public Boolean execute() {
        attemptTrayTestStateCorrection();
        return true;
    }

    private void attemptTrayTestStateCorrection() {
        List<TrayConnectorTest> unfinishedTrayTests = trayConnectorTestService.findUnfinishedTests();

        log.info("Found " + unfinishedTrayTests.size() + " Tray tests currently unfinished");
        List<TrayConnectorTest> timedOutTrayTests;

        // Tray tests timeout:
        // AD_PLATFORM in 3 days (1 day for processing + 2 days for audience size update)
        // Other destinations in 1 day
        timedOutTrayTests = unfinishedTrayTests.stream()
                .filter(
                    test -> (isAdPlatform(test) && test.getStartTime().toInstant().atOffset(ZoneOffset.UTC)
                                    .isBefore(Instant.now().atOffset(ZoneOffset.UTC).minusHours(72))) ||
                            (!isAdPlatform(test) && test.getStartTime().toInstant().atOffset(ZoneOffset.UTC)
                                    .isBefore(Instant.now().atOffset(ZoneOffset.UTC).minusHours(24)))
                    )
                .collect(Collectors.toList());

        if (timedOutTrayTests.size() > 0) {
            log.info(timedOutTrayTests.size() + " Tray tests timed out");

            try {
                clearTimedOutTrayTests(timedOutTrayTests);
            } catch (Exception e) {
                log.error("Failed to clear Tray tests", e);
            }
        }
    }

    private void clearTimedOutTrayTests(List<TrayConnectorTest> timedOutTrayTests) {
        timedOutTrayTests.forEach(test -> {
            trayConnectorTestService.cancelTrayTestByWorkflowReqId(test.getWorkflowRequestId());
        });
    }

    private boolean isAdPlatform(TrayConnectorTest test) {
        return trayConnectorTestService.isAdPlatform(test);
    }

}
