package com.latticeengines.cdl.workflow.steps.importdata;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.ImportStatus;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.RegisterExtractConfiguration;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("registerExtract")
public class RegisterExtract extends BaseWorkflowStep<RegisterExtractConfiguration> {

    private final static Logger log = LoggerFactory.getLogger(RegisterExtract.class);

    @Autowired
    private EaiJobDetailProxy eaiJobDetailProxy;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Override
    public void execute() {
        EaiImportJobDetail eaiImportJobDetail = eaiJobDetailProxy.getImportJobDetailByCollectionIdentifier(configuration
                .getImportJobIdentifier());
        if (eaiImportJobDetail == null || eaiImportJobDetail.getStatus() != ImportStatus.WAITINGREGISTER) {
            if (eaiImportJobDetail == null) {
                log.warn(String.format("Cannot find the job detail for %s", configuration.getImportJobIdentifier()));
            } else  {
                log.warn(String.format("Last import detail status is %s instead of WAITINGREGISTER",
                        eaiImportJobDetail.getStatus().name()));
            }
            return;
        }
        try {
            String templateName = eaiImportJobDetail.getTemplateName();
            List<String> pathList = eaiImportJobDetail.getPathDetail();
            List<String> processedRecordsList = eaiImportJobDetail.getPRDetail();
            if (pathList == null || processedRecordsList == null || pathList.size() != processedRecordsList.size()) {
                log.error("Error in extract info, skip register extract.");
                updateEaiImportJobDetail(eaiImportJobDetail, ImportStatus.FAILED);
                return;
            }
            List<Extract> extracts = new ArrayList<>();
            for (int i = 0; i < pathList.size(); i++) {
                log.info(String.format("Extract %s have %s records.", pathList.get(i), processedRecordsList.get(i)));
                long records = Long.parseLong(processedRecordsList.get(i));
                extracts.add(createExtract(pathList.get(i), records));
            }
            if (extracts.size() == 1) {
                log.info(String.format("Register single extract: %s", extracts.get(0).getName()));
                dataFeedProxy.registerExtract(configuration.getCustomerSpace().toString(),
                                            configuration.getImportJobIdentifier(), templateName, extracts.get(0));
            } else {
                log.info(String.format("Register %d extracts.", extracts.size()));
                dataFeedProxy.registerExtracts(configuration.getCustomerSpace().toString(),
                        configuration.getImportJobIdentifier(), templateName, extracts);
            }
            updateEaiImportJobDetail(eaiImportJobDetail, ImportStatus.SUCCESS);
        } catch (Exception e) {
            updateEaiImportJobDetail(eaiImportJobDetail, ImportStatus.FAILED);
        }
    }

    private void updateEaiImportJobDetail(EaiImportJobDetail eaiImportJobDetail, ImportStatus importStatus) {
        eaiImportJobDetail.setStatus(importStatus);
        eaiJobDetailProxy.updateImportJobDetail(eaiImportJobDetail);
    }


    private Extract createExtract(String path, long processedRecords) {
        Extract e = new Extract();
        e.setName(StringUtils.substringAfterLast(path, "/"));
        e.setPath(PathUtils.stripoutProtocol(path));
        e.setProcessedRecords(processedRecords);
        String dateTime = StringUtils.substringBetween(path, "/Extracts/", "/");
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        try {
            e.setExtractionTimestamp(f.parse(dateTime).getTime());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return e;
    }
}
