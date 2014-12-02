package com.latticeengines.dataplatform.runtime.eai;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.eai.DataExtractionConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.exposed.service.DataExtractionService;
import com.latticeengines.eai.routes.ImportProperty;

public class EaiProcessor implements ItemProcessor<List<Table>, String> {

    private static final Log log = LogFactory.getLog(EaiProcessor.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private DataExtractionService dataExtractionService;

    private String targetPath;

    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }

    @Override
    public String process(List<Table> item) throws Exception {
        ImportContext context = new ImportContext();
        context.setProperty(ImportProperty.HADOOPCONFIG, yarnConfiguration);
        context.setProperty(ImportProperty.TARGETPATH, targetPath);
        DataExtractionConfiguration extractionConfig = new DataExtractionConfiguration();
        extractionConfig.setTables(item);
        dataExtractionService.extractAndImport(extractionConfig, context);
        Thread.sleep(10000L);
        return null;
    }

}
