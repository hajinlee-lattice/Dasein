package com.latticeengines.eai.yarn.runtime;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;

@Component("importProcessor")
public class ImportProcessor extends SingleContainerYarnProcessor<ImportConfiguration>
        implements ItemProcessor<ImportConfiguration, String> {

    @Autowired
    private ImportTableProcessor importTableProcessor;

    @Override
    public String process(ImportConfiguration importConfig) throws Exception {
        return importTableProcessor.process(importConfig);
    }

}
