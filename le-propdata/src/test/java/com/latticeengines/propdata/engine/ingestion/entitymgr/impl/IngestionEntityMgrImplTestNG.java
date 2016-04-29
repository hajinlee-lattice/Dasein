package com.latticeengines.propdata.engine.ingestion.entitymgr.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.domain.exposed.propdata.ingestion.SftpProtocol;
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
        Assert.assertNotNull(ingestion.getProtocol(), "Failed to parse proptocol from source");
        Assert.assertEquals(ingestion.getIngestionType(), IngestionType.SFTP_TO_HDFS);
        SftpProtocol sftpConfig = (SftpProtocol) ingestion.getProtocol();
        log.info("SFTP configuration: " + sftpConfig.getSftpHost() + ":" + sftpConfig.getSftpPort()
                + " " + sftpConfig.getSftpUserName() + "/" + sftpConfig.getSftpPasswordEncrypted()
                + " " + sftpConfig.getSftpDir());

        List<String> files = sftpConfig.getAllFiles();
        Assert.assertNotNull(files);
        Assert.assertNotEquals(files.isEmpty(), true);
        StringBuilder sb = new StringBuilder();
        sb.append("Files under sftp directory " + sftpConfig.getSftpDir() + ": ");
        for (String file : files) {
            sb.append(file + ", ");
        }
        log.info(sb.toString());
        log.info("Ingestion configuration: " + ingestion.toString());
    }

}
