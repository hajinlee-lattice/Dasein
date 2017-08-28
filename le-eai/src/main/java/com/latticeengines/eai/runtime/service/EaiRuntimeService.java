package com.latticeengines.eai.runtime.service;

import java.lang.reflect.ParameterizedType;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ImportStatus;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.eai.service.EaiImportJobDetailService;

public abstract class EaiRuntimeService<T extends EaiJobConfiguration> {

    private static Map<Class<? extends EaiJobConfiguration>, EaiRuntimeService<? extends EaiJobConfiguration>> map = new HashMap<>();

    protected Function<Float, Void> progressReporter;

    @Autowired
    private EaiImportJobDetailService eaiImportJobDetailService;

    @SuppressWarnings("unchecked")
    public EaiRuntimeService() {
        map.put((Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0], this);
    }

    public static EaiRuntimeService<? extends EaiJobConfiguration> getRunTimeService(
            Class<? extends EaiJobConfiguration> clz) {
        return map.get(clz);
    }

    //public abstract void initailize(T config);

    public abstract void invoke(T config);

    //public abstract void finalize(T config);

    public void setProgressReporter(Function<Float, Void> progressReporter) {
        this.progressReporter = progressReporter;
    }

    protected void setProgress(float progress) {
        progressReporter.apply(progress);
    }

    public void initJobDetail(String jobIdentifier, SourceType sourceType) {
        EaiImportJobDetail jobDetail = eaiImportJobDetailService
                .getImportJobDetailByCollectionIdentifier(jobIdentifier);
        if (jobDetail == null) {
            jobDetail = new EaiImportJobDetail();
            jobDetail.setStatus(ImportStatus.SUBMITTED);
            jobDetail.setSourceType(sourceType);
            jobDetail.setCollectionIdentifier(jobIdentifier);
            jobDetail.setProcessedRecords(0);
            jobDetail.setCollectionTimestamp(new Date());
            eaiImportJobDetailService.createImportJobDetail(jobDetail);
        } else {
            jobDetail.setStatus(ImportStatus.SUBMITTED);
            eaiImportJobDetailService.updateImportJobDetail(jobDetail);
        }
    }

    public void updateJobDetailExtractInfo(String jobIdentifier, String templateName, List<String> pathList,
                                           List<String> processedRecords) {
        EaiImportJobDetail jobDetail = eaiImportJobDetailService
                .getImportJobDetailByCollectionIdentifier(jobIdentifier);
        if (jobDetail != null) {
            jobDetail.setTemplateName(templateName);
            jobDetail.setPathDetail(pathList);
            jobDetail.setPRDetail(processedRecords);
            eaiImportJobDetailService.updateImportJobDetail(jobDetail);
        }
    }

}
