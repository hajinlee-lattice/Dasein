package com.latticeengines.datacloud.match.service.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.exposed.service.DomainCollectService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;

@Component("domainCollectService")
public class DomainCollectServiceImpl implements DomainCollectService {

    private static final Logger log = LoggerFactory.getLogger(DomainCollectServiceImpl.class);
    private static final String REQ_PROVIDERS = "DerivedColumns";
    private static final List<String> INSERT_COLS = Arrays.asList( //
            "[TransferProcess_ID]", //
            "[LEAccount_ID]", //
            "[External_ID]", //
            "[Name_Category]", //
            "[Domain_Name]", //
            "[Row_Num]", //
            "[Creation_Date]");
    private static final Set<String> domainSet = new ConcurrentSkipListSet<>();
    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);
    private static final int BUFFER_SIZE = 800;
    private static final int MAX_DUMP_SIZE = 40000;

    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Autowired
    @Qualifier("commonTaskScheduler")
    private ThreadPoolTaskScheduler scheduler;

    @Autowired
    @Qualifier("dataCloudCollectorJdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    @Value("${datacloud.collector.enabled}")
    private boolean domainCollectEnabled;

    @PostConstruct
    public void postConstruct() {
        if (domainCollectEnabled) {
            scheduler.scheduleWithFixedDelay(this::dumpQueue, TimeUnit.MINUTES.toMillis(10));
        }
    }

    @Override
    public void enqueue(String domain) {
        if (domainCollectEnabled) {
            domainSet.add(domain);
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
                for (String domain : domains) {
                    domainBuffer.add(domain);
                    if (domainBuffer.size() >= BUFFER_SIZE) {
                        log.info(
                                "Dumping " + domainBuffer.size() + " domains in the buffer to collector's url stream.");
                        putDomainsInAccountTransferTable(transferId, domainBuffer);
                        domainBuffer = new HashSet<>();
                    }
                }
                if (!domainBuffer.isEmpty()) {
                    log.info("Dumping " + domainBuffer.size() + " domains in the buffer to collector's url stream.");
                    putDomainsInAccountTransferTable(transferId, domainBuffer);
                }
                executeDomainCollectionTransfer(transferId);
            }
        }
    }

    private void putDomainsInAccountTransferTable(String transferId, Collection<String> domains) {
        String sql = constructSql(transferId, domains);
        jdbcTemplate.execute(sql);
    }

    private void executeDomainCollectionTransfer(String transferId) {
        StringBuilder sb = new StringBuilder("EXEC [dbo].[MatcherService_HandleDomainCollectionTransfer_3]");
        sb.append(String.format(" '%s',", transferId));
        sb.append(String.format(" '%s',", transferId));
        sb.append(String.format(" '%s',", DataCloudConstants.SERVICE_TENANT));
        sb.append(String.format(" '%s',", DataCloudConstants.SERVICE_TENANT));
        sb.append(String.format(" '%s';", REQ_PROVIDERS));
        String sql = sb.toString();
        jdbcTemplate.execute(sql);
    }

    private String constructSql(String transferId, Collection<String> domains) {
        StringBuilder sb = new StringBuilder("INSERT INTO [LE_AccountTransferTable] (");
        sb.append(StringUtils.join(INSERT_COLS, ", "));
        sb.append(") VALUES\n");

        List<String> values = new ArrayList<>();
        int rowNum = 0;

        Date date = new Date(System.currentTimeMillis());
        String createDate = dateFormat.format(date);

        for (String domain : domains) {
            if (StringUtils.isNotBlank(domain)) {
                values.add(convertToValue(domain, transferId, rowNum++, createDate));
            }
        }
        sb.append(StringUtils.join(values, ",\n"));
        sb.append(";");
        return sb.toString();
    }

    private String convertToValue(String domain, String transferId, int rowNum, String createDate) {
        return String.format("('%s', '%s', '%s', 'DOMAIN', '%s', %d, '%s')", //
                transferId, //
                String.valueOf(rowNum), //
                UUID.randomUUID().toString(), //
                domain, //
                rowNum, //
                createDate);
    }

    public int getQueueSize() {
        return domainSet.size();
    }

}
