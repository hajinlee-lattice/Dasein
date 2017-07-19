package com.latticeengines.eai.yarn.runtime;

import java.util.List;
import java.util.Optional;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.ImportStatus;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.eai.runtime.service.EaiRuntimeService;
import com.latticeengines.eai.service.EaiImportJobDetailService;
import com.latticeengines.yarn.exposed.runtime.SingleContainerYarnProcessor;

@Component("importProcessor")
public class ImportProcessor extends SingleContainerYarnProcessor<ImportConfiguration>
        implements ItemProcessor<ImportConfiguration, String> {

    // @Autowired
    // private ImportTableProcessor importTableProcessor;

    @Autowired
    private ImportVdbTableProcessor importVdbTableProcessor;

    @Autowired
    private EaiImportJobDetailService eaiImportJobDetailService;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public String process(ImportConfiguration importConfig) throws Exception {
        Optional<SourceImportConfiguration> sourceImportConfiguration = importConfig.getSourceConfigurations().stream()
                .findFirst();
        if (sourceImportConfiguration.isPresent()
                && sourceImportConfiguration.get().getSourceType().equals(SourceType.VISIDB)) {
            String result = importVdbTableProcessor.process(importConfig);
            finalizeImportJob(importConfig);
            return result;
        }
        EaiRuntimeService eaiRuntimeService = EaiRuntimeService.getRunTimeService(importConfig.getClass());
        eaiRuntimeService.setProgressReporter(progress -> {
            setProgress((Float) progress);
            return null;
        });
        eaiRuntimeService.invoke(importConfig);
        return null;

    }

    public void finalizeImportJob(ImportConfiguration importConfig) {
        String collectionIdentifiers = importConfig.getProperty(ImportProperty.COLLECTION_IDENTIFIERS);
        if (!StringUtils.isEmpty(collectionIdentifiers)) {
            @SuppressWarnings("unchecked")
            List<Object> identifiersRaw = JsonUtils.deserialize(collectionIdentifiers, List.class);
            List<String> identifiers = JsonUtils.convertList(identifiersRaw, String.class);
            // List<EaiImportJobDetail> jobDetails = new ArrayList<>();
            for (String collectionIdentifier : identifiers) {
                EaiImportJobDetail eaiImportJobDetail = eaiImportJobDetailService
                        .getImportJobDetail(collectionIdentifier);
                if (eaiImportJobDetail != null) {
                    eaiImportJobDetail.setStatus(ImportStatus.SUCCESS);
                    eaiImportJobDetailService.updateImportJobDetail(eaiImportJobDetail);
                }
            }
        }
    }

}
