package com.latticeengines.apps.cdl.service.impl;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.InboundConnectionService;
import com.latticeengines.apps.cdl.service.MockBrokerInstanceService;
import com.latticeengines.apps.cdl.service.SQSMessageScanService;
import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.bean.BeanFactoryEnvironment;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.MockBrokerInstance;
import com.latticeengines.domain.exposed.jms.S3ImportMessageType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.pls.PlsHealthCheckProxy;

@Component("sqsMessageScanService")
public class SQSMessageScanServiceImpl implements SQSMessageScanService {

    private static final Logger log = LoggerFactory.getLogger(SQSMessageScanServiceImpl.class);

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${cdl.s3.inbound.connector.scan.period.seconds}")
    private int scanPeriod;

    @Value("${common.le.stack}")
    private String currentStack;

    @Inject
    private PlsHealthCheckProxy plsHealthCheckProxy;

    @Inject
    private ZKConfigService zkConfigService;

    @Inject
    private MockBrokerInstanceService mockBrokerInstanceService;

    @Inject
    private InboundConnectionService inboundConnectionService;

    private ImportAggregateRunner runner;

    private volatile boolean active = true;
    private String activeStackName;
    private boolean isActiveStack = false;

    private static final String SQS_SCAN_LOCK = "sqsScanLock";
    private static final String CURRENT_STACK = "CurrentStack";

    @PostConstruct
    @DependsOn("beanEnvironment")
    public void initialize() {
        if (BeanFactoryEnvironment.Environment.WebApp.equals(BeanFactoryEnvironment.getEnvironment())) {
            log.info("SQSMessageScanService started.");
            String hostAddress = getHostAddress();
            CuratorFramework client = CamilleEnvironment.getCamille().getCuratorClient();
            String lockPath = PathBuilder.buildLockPath(CamilleEnvironment.getPodId(),
                    CamilleEnvironment.getDivision(), SQS_SCAN_LOCK).toString();
//            runner = new ImportAggregateRunner(client, lockPath, leStack, hostAddress);
//            runner.start();
        }
    }

    public String getHostAddress() {
        String hostAddress = "localhost";
        try {
            hostAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            log.error("Can't get host address: ", e);
        }
        return hostAddress;
    }

    @PreDestroy
    public void preDestroy() {
        active = false;
        if (runner != null) {
            CloseableUtils.closeQuietly(runner);
        }
    }

    private class ImportAggregateRunner extends LeaderSelectorListenerAdapter implements Closeable {

        private final String stackName;
        private final String hostAddress;
        private final LeaderSelector leaderSelector;

        ImportAggregateRunner(CuratorFramework client, String path, String stackName, String hostAddress) {
            this.leaderSelector = new LeaderSelector(client, path, this);
            this.leaderSelector.autoRequeue();
            this.stackName = stackName;
            this.hostAddress = hostAddress;
        }

        public void start() {
            leaderSelector.start();
        }

        @Override
        public void close() {
            leaderSelector.close();
        }

        @Override
        public void takeLeadership(CuratorFramework curatorFramework) {
            log.info(String.format("Instance with Ip %s in stack %s start to scan sqs message.", hostAddress, stackName));
            while (active) {
                try {
                    long totalTime = scanPeriod * 1000;
                    if (StringUtils.isNotEmpty(activeStackName) || successGetActiveStackInfo()) {
                        StopWatch stopWatch = StopWatch.createStarted();
                        List<MockBrokerInstance> mockBrokerInstances = mockBrokerInstanceService.getAllValidInstance(new Date());
                        if (CollectionUtils.isNotEmpty(mockBrokerInstances)) {
                            for (MockBrokerInstance mockBrokerInstance : mockBrokerInstances) {
                                Tenant tenant = mockBrokerInstance.getTenant();
                                CustomerSpace space = CustomerSpace.parse(tenant.getId());
                                String stackName = zkConfigService.getStack(space);
                                S3ImportMessageType messageType = zkConfigService.getTriggerName(space);
                                if (scheduleOnCurrentStack(stackName, messageType)) {
                                    inboundConnectionService.submitMockBrokerAggregationWorkflow();
                                }
                            }
                        }
                        long executionTime = stopWatch.getTime();
                        if (executionTime < totalTime) {
                            long sleepTime = totalTime - executionTime;
                            log.info("Aggregate task is done, need to sleep {} ms.", sleepTime);
                            TimeUnit.MILLISECONDS.sleep(sleepTime);
                        }
                    } else {
                        log.info("Can not get stack info, wait pls service to start up");
                        TimeUnit.MILLISECONDS.sleep(totalTime);
                    }
                } catch (Exception e) {
                    log.error(String.format("Instance with Ip %s in stack %s will lost leader ship with exception: ", hostAddress, stackName), e.getMessage());
                    break;
                }
            }
        }
    }

    private boolean successGetActiveStackInfo() {
        try {
            Map<String, String> stackInfo = plsHealthCheckProxy.getActiveStack();
            if (MapUtils.isNotEmpty(stackInfo) && stackInfo.containsKey(CURRENT_STACK)) {
                activeStackName = stackInfo.get(CURRENT_STACK);
                isActiveStack = currentStack.equalsIgnoreCase(activeStackName);
            }
            log.info("activeStackName is {}, currentStack is {}, isActiveStack is {}.", activeStackName, currentStack, isActiveStack);
            return true;
        } catch (Exception e) {
            log.error("Can't get active stack info:", e.getMessage());
            return false;
        }
    }

    private boolean scheduleOnCurrentStack(String stackName, S3ImportMessageType messageType) {
        boolean runOnActiveStack = StringUtils.isEmpty(stackName) || stackName.equalsIgnoreCase(activeStackName);
        if (runOnActiveStack) {
            if (isActiveStack) {
                return true;
            } else {
                return false;
            }
        } else if (currentStack.equalsIgnoreCase(stackName) && S3ImportMessageType.INBOUND_CONNECTION.equals(messageType)) {
            return true;
        } else {
            return false;
        }
    }
}
