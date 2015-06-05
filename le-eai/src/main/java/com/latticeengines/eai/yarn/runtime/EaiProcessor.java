package com.latticeengines.eai.yarn.runtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.eai.routes.ImportProperty;
import com.latticeengines.eai.service.DataExtractionService;

@Component
public class EaiProcessor implements ItemProcessor<ImportConfiguration, String> {

    private static final Log log = LogFactory.getLog(EaiProcessor.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private DataExtractionService dataExtractionService;

    @Override
    public String process(ImportConfiguration importConfig) throws Exception {
        ImportContext context = new ImportContext();
        context.setProperty(ImportProperty.HADOOPCONFIG, yarnConfiguration);
        context.setProperty(ImportProperty.TARGETPATH, importConfig.getTargetPath());
        log.info("Starting extract and import.");
        dataExtractionService.extractAndImport(importConfig, context);
        log.info("Finished extract and import.");
        Thread.sleep(10000L);
        return null;
    }

}
