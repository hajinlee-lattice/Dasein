package com.latticeengines.propdata.engine.ingestion.entitymgr.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.domain.exposed.propdata.ingestion.SftpConfiguration;
import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.domain.exposed.propdata.manage.Ingestion.IngestionType;
import com.latticeengines.propdata.engine.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.propdata.engine.testframework.PropDataEngineFunctionalTestNGBase;

@Component
public class IngestionEntityMgrImplTestNG extends PropDataEngineFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(IngestionEntityMgrImplTestNG.class);

    private static final String INGESTION_NAME = "BomboraFirehose";

    @Autowired
    private IngestionEntityMgr ingestionEntityMgr;

    @Test(groups = "functional", enabled = true)
    public void testIngestion() throws JsonProcessingException {
        Ingestion ingestion = ingestionEntityMgr.getIngestionByName(INGESTION_NAME);
        Assert.assertNotNull(ingestion, "Failed to get ingestion configuration");
        Assert.assertNotNull(ingestion.getProviderConfiguration(),
                "Failed to parse provider configuration from source");
        Assert.assertEquals(ingestion.getIngestionType(), IngestionType.SFTP_TO_HDFS);
        SftpConfiguration sftpConfig = (SftpConfiguration) ingestion.getProviderConfiguration();
        log.info("SFTP configuration: " + sftpConfig.getSftpHost() + ":" + sftpConfig.getSftpPort()
                + " " + sftpConfig.getSftpUserName() + "/" + sftpConfig.getSftpPasswordEncrypted()
                + " " + sftpConfig.getSftpDir());
        log.info("Ingestion configuration: " + ingestion.toString());
    }

}
