package com.latticeengines.apps.cdl.tray.service.impl;

import java.io.InputStream;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.tray.entitymgr.TrayConnectorTestEntityMgr;
import com.latticeengines.apps.cdl.tray.service.TrayConnectorTestService;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.aws.sns.SNSService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.ExternalIntegrationMessageBody;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTestMetadata;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTestMetadata.TriggerConfig;
import com.latticeengines.domain.exposed.cdl.tray.TrayConnectorTestMetadata.TriggerMetadata;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;

@Component("trayConnectorTestService")
public class TrayConnectorTestServiceImpl implements TrayConnectorTestService {

    private static final Logger log = LoggerFactory.getLogger(TrayConnectorTestServiceImpl.class);

    @Inject
    private TrayConnectorTestEntityMgr trayConnectorTestEntityMgr;

    @Inject
    private SNSService snsService;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private S3Service s3Service;

    @Inject
    private DynamoItemService dynamoItemService;

    @Value("${aws.data.integration.exportdata.topic}")
    private String exportDataTopic;

    @Value("${aws.customer.export.s3.bucket}")
    private String exportS3Bucket;

    @Value("${cdl.tray.test.metadata.bucket}")
    private String trayTestMetadataBucket;

    private String pathTemplate = "connectors/%s/test-scenarios/%s/input.json";

    @Override
    public void createTrayConnectorTest(CDLExternalSystemName externalSystemName, String testScenario) {

        String objectKey = String.format(pathTemplate, externalSystemName, testScenario);
        log.info("objectKey=" + objectKey);
        // read test metadata file from S3
        InputStream is = s3Service.readObjectAsStream(trayTestMetadataBucket, objectKey);
        TrayConnectorTestMetadata metadata = JsonUtils.deserialize(is, TrayConnectorTestMetadata.class);

        // create SNS message
        TriggerMetadata trigger = metadata.getTrigger();
        TriggerConfig triggerConfig = trigger.getTriggerConfig();

        // TODO generate solution instnace
        if (triggerConfig.getGenerateSolutionInstance()) {

        }

        // TODO generate external audience
        if (triggerConfig.getGenerateExternalAudience()) {

        }

        ExternalIntegrationMessageBody message = trigger.getMessage();

        // register in DynamoDB

        // register in TrayConnectorTest table

    }

}
