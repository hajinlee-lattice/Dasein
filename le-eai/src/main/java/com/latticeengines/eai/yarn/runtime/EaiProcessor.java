package com.latticeengines.eai.yarn.runtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;

public class EaiProcessor extends SingleContainerYarnProcessor<ImportConfiguration> implements
        ItemProcessor<ImportConfiguration, String>, ApplicationContextAware {

    private static final Log log = LogFactory.getLog(EaiProcessor.class);

    @Autowired
    private ImportTableProcessor importTableProcessor;

    @Autowired
    private CamelRouteProcessor camelRouteProcessor;

    @Override
    public String process(ImportConfiguration importConfig) throws Exception {
        ImportConfiguration.ImportType importType = importConfig.getImportType();
        if (importType == null) {
            importType = ImportConfiguration.ImportType.ImportTable;
        }

        switch (importType) {
            case CamelRoute:
                log.info("Directing import job to " + camelRouteProcessor.getClass().getSimpleName());
                return camelRouteProcessor.process(importConfig);
            case ImportTable:
            default:
                log.info("Directing import job to " + importTableProcessor.getClass().getSimpleName());
                return importTableProcessor.process(importConfig);
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        importTableProcessor.setApplicationContext(applicationContext);
        camelRouteProcessor.setApplicationContext(applicationContext);
    }
}
