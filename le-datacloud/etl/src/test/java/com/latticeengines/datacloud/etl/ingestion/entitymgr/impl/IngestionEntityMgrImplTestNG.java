package com.latticeengines.datacloud.etl.ingestion.entitymgr.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.datacloud.etl.testframework.DataCloudEtlFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.ingestion.SftpConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion.IngestionType;

@Component
public class IngestionEntityMgrImplTestNG extends DataCloudEtlFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(IngestionEntityMgrImplTestNG.class);

    private static final String INGESTION_NAME = "BomboraFirehose";

    @Autowired
    private IngestionEntityMgr ingestionEntityMgr;

    @Test(groups = "functional", enabled = true)
    public void testIngestion() throws JsonProcessingException {
        Ingestion ingestion = ingestionEntityMgr.getIngestionByName(INGESTION_NAME);
        Assert.assertNotNull(ingestion, "Failed to get ingestion configuration");
        Assert.assertNotNull(ingestion.getProviderConfiguration(),
                "Failed to parse provider configuration from source");
        Assert.assertEquals(ingestion.getIngestionType(), IngestionType.SFTP);
        SftpConfiguration sftpConfig = (SftpConfiguration) ingestion.getProviderConfiguration();
        log.info("SFTP configuration: " + sftpConfig.getSftpHost() + ":" + sftpConfig.getSftpPort()
                + " " + sftpConfig.getSftpUserName() + "/" + sftpConfig.getSftpPasswordEncrypted()
                + " " + sftpConfig.getSftpDir());
        log.info("Ingestion configuration: " + ingestion.toString());
    }

}
