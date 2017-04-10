package com.latticeengines.eai.yarn.runtime;

import java.util.Optional;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;

@Component("importProcessor")
public class ImportProcessor extends SingleContainerYarnProcessor<ImportConfiguration>
        implements ItemProcessor<ImportConfiguration, String> {

    @Autowired
    private ImportTableProcessor importTableProcessor;

    @Autowired
    private ImportVdbTableProcessor importVdbTableProcessor;

    @Override
    public String process(ImportConfiguration importConfig) throws Exception {
        Optional<SourceImportConfiguration> sourceImportConfiguration =  importConfig.getSourceConfigurations()
                .stream().findFirst();
        if (sourceImportConfiguration.isPresent()
                && sourceImportConfiguration.get().getSourceType().equals(SourceType.VISIDB)) {
            return importVdbTableProcessor.process(importConfig);
        } else {
            return importTableProcessor.process(importConfig);
        }
    }

}
