package com.latticeengines.datacloud.match.service.impl;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.exposed.service.DomainCollectService;
import com.latticeengines.ldc_collectiondb.entitymgr.RawCollectionRequestMgr;

@Component("domainCollectService")
public class DomainCollectServiceImpl implements DomainCollectService {

    private static final Logger log = LoggerFactory.getLogger(DomainCollectServiceImpl.class);
    private static final Set<String> domainSet = new ConcurrentSkipListSet<>();
    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);
    private static final int BUFFER_SIZE = 800;
    private static final int MAX_DUMP_SIZE = 40000;
    private static final long MS_IN_MIN = 1000 * 60;

    private AtomicLong droppedEnqDomain = new AtomicLong(0);
    private static final int LOG_DROP_NUM = 1000;

    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Autowired
    @Qualifier("commonTaskScheduler")
    private ThreadPoolTaskScheduler scheduler;

    @Autowired
    private RawCollectionRequestMgr rawCollectionRequestMgr;

    @Value("${datacloud.collector.enabled}")
    private boolean domainCollectEnabled;

    private boolean drainMode = false;

    @PostConstruct
    public void postConstruct() {
        if (domainCollectEnabled) {
            scheduler.scheduleWithFixedDelay(this::dumpQueue, TimeUnit.MINUTES.toMillis(10));
        }
    }

    @Override
    public void enqueue(String domain) {
        if (!domainCollectEnabled) {
            return;
        }
        if (domainSet.size() < MAX_DUMP_SIZE) {
            domainSet.add(domain);
        } else {
            long cnt = droppedEnqDomain.incrementAndGet();
            if (cnt % LOG_DROP_NUM == 0) {
                log.warn("Have dropped enqueued domains: DroppedEnqDomain=" + cnt);
            }
        }
    }

    @Override
    public void dumpQueue() {
        if (domainCollectEnabled) {
            Set<String> domains = new HashSet<>();
            synchronized (domainSet) {
                if (domainSet.size() > MAX_DUMP_SIZE) {
                    domains.addAll(domainSet.stream().limit(MAX_DUMP_SIZE).collect(Collectors.toList()));
                    domainSet.removeAll(domains);
                    log.warn(String.format("Too many domains (%d) to dump, keep only %d, drop the remaining.",
                            domainSet.size() + domains.size(), domains.size()));
                } else {
                    domains.addAll(domainSet);
                    domainSet.clear();
                }
            }
            if (!domains.isEmpty()) {
                Set<String> domainBuffer = new HashSet<>();
                String transferId = UUID.randomUUID().toString();
                log.info("Splitting " + domains.size() + " domains to be inserted into collector's url stream.");
                long timestamp = System.currentTimeMillis();
                int dumpCnt = 0;
                if (drainMode) {
                    log.info("In draining mode, only dump domains for 2 mins and drop remaining domains");
                }
                for (String domain : domains) {
                    domainBuffer.add(domain);
                    if (domainBuffer.size() >= BUFFER_SIZE) {
                        log.info(
                                "Dumping " + domainBuffer.size() + " domains in the buffer to collector's url stream.");
                        dumpDomains(transferId, domainBuffer);
                        domainBuffer = new HashSet<>();
                        dumpCnt += domainBuffer.size();
                    }
                    if (drainMode && System.currentTimeMillis() - timestamp > 2 * MS_IN_MIN) {
                        break;
                    }
                }
                if (!domainBuffer.isEmpty() && !(drainMode && System.currentTimeMillis() - timestamp > 2 * MS_IN_MIN)) {
                    log.info("Dumping " + domainBuffer.size() + " domains in the buffer to collector's url stream.");
                    dumpDomains(transferId, domainBuffer);
                    dumpCnt += domainBuffer.size();
                }
                log.info("Finished dumping " + dumpCnt + " domains to collector's url stream.");
            }
        }
    }

    private void dumpDomains(String transferId, Collection<String> domains) {
        rawCollectionRequestMgr.saveRequests(domains, transferId);
    }

    public int getQueueSize() {
        return domainSet.size();
    }

    public void setDrainMode() {
        drainMode = true;
    }

}
