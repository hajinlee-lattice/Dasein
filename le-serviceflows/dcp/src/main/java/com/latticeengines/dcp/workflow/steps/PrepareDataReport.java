package com.latticeengines.dcp.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.Classification;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.MatchedDuns;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.DunsCountCache;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.RollupDataReportStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.dcp.PrepareDataReportConfig;
import com.latticeengines.proxy.exposed.dcp.DataReportProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.dcp.PrepareDataReportJob;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PrepareDataReport extends RunSparkJob<RollupDataReportStepConfiguration, PrepareDataReportConfig> {

    private static final Logger log = LoggerFactory.getLogger(PrepareDataReport.class);

    @Inject
    private DataReportProxy dataReportProxy;

    @Inject
    private UploadProxy uploadProxy;

    private Map<Integer, String> indexToUploadId;

    @Override
    protected Class<? extends AbstractSparkJob<PrepareDataReportConfig>> getJobClz() {
        return PrepareDataReportJob.class;
    }

    @Override
    protected PrepareDataReportConfig configureJob(RollupDataReportStepConfiguration stepConfiguration) {
        DataReportRecord.Level level = stepConfiguration.getLevel();
        String rootId = stepConfiguration.getRootId();
        Set<String> uploadIds = getUploadIds(level, rootId);
        List<DataUnit> inputs = new ArrayList<>();
        indexToUploadId = new HashMap<>();
        int index = 0;
        for (String uploadId : uploadIds) {
            DunsCountCache cache = dataReportProxy.getDunsCount(customerSpace.toString(), DataReportRecord.Level.Upload,
                    uploadId);
            if (StringUtils.isBlank(cache.getDunsCountTableName()) && cache.getSnapshotTimestamp() == null) {
                String resultTableName = uploadProxy.getMatchResultTableName(customerSpace.toString(), uploadId);
                Table table = metadataProxy.getTable(customerSpace.toString(), resultTableName);
                inputs.add(table.toHdfsDataUnit(String.format("input%s", index)));
                indexToUploadId.put(index, uploadId);
                index++;
            }
        }
        PrepareDataReportConfig config = new PrepareDataReportConfig();
        config.setInput(inputs);
        config.setClassificationAttr(Classification);
        config.setMatchedDunsAttr(MatchedDuns);
        config.setNumTargets(indexToUploadId.size());
        return config;
    }

    private Set<String> getUploadIds(DataReportRecord.Level level, String rootId) {
        Set<String> uploadIds = new HashSet<>();
        switch (level) {
            case Tenant:
                Set<String> projectIds = dataReportProxy.getChildrenIds(customerSpace.toString(), level, rootId);
                projectIds.forEach(projectId -> {
                    Set<String> sourceIds = dataReportProxy.getChildrenIds(customerSpace.toString(),
                            DataReportRecord.Level.Project, projectId);
                    sourceIds.forEach(sourceId -> {
                        Set<String> retrievedUploadIds = dataReportProxy.getChildrenIds(customerSpace.toString(),
                                DataReportRecord.Level.Source, sourceId);
                        retrievedUploadIds.forEach(uploadId -> {
                            DunsCountCache cache = dataReportProxy.getDunsCount(customerSpace.toString(),
                                    DataReportRecord.Level.Upload, uploadId);
                            if (cache.getSnapshotTimestamp() == null) {
                                uploadIds.add(uploadId);
                            }
                        });
                    });
                });
                break;
            case Project:
                Set<String> sourceIds = dataReportProxy.getChildrenIds(customerSpace.toString(), level, rootId);
                sourceIds.forEach(sourceId -> {
                    Set<String> retrievedUploadIds = dataReportProxy.getChildrenIds(customerSpace.toString(),
                            DataReportRecord.Level.Source, sourceId);
                    retrievedUploadIds.forEach(uploadId -> {
                        DunsCountCache cache = dataReportProxy.getDunsCount(customerSpace.toString(),
                                DataReportRecord.Level.Upload, uploadId);
                        if (cache.getSnapshotTimestamp() == null) {
                            uploadIds.add(uploadId);
                        }
                    });
                });
                break;
            case Source:
                Set<String> retrievedUploadIds = dataReportProxy.getChildrenIds(customerSpace.toString(), level,
                        rootId);
                retrievedUploadIds.forEach(uploadId -> {
                    DunsCountCache cache = dataReportProxy.getDunsCount(customerSpace.toString(),
                            DataReportRecord.Level.Upload, uploadId);
                    if (cache.getSnapshotTimestamp() == null) {
                        uploadIds.add(uploadId);
                    }
                });
                break;
            default:
                break;
        }
        return uploadIds;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        List<HdfsDataUnit> units = result.getTargets();
        for (Integer index : indexToUploadId.keySet()) {
            String uploadId = indexToUploadId.get(index);
            HdfsDataUnit unit = units.get(index);
            String dunsCountTableName = NamingUtils.timestamp(String.format("dunsCount_%s", uploadId));
            Table dunsCount = toTable(dunsCountTableName, null, unit);
            metadataProxy.createTable(configuration.getCustomerSpace().toString(), dunsCountTableName, dunsCount);
            registerTable(dunsCountTableName);
            DataReport report = dataReportProxy.getDataReport(customerSpace.toString(), DataReportRecord.Level.Upload,
                    uploadId);
            DunsCountCache cache = new DunsCountCache();
            cache.setDunsCountTableName(dunsCountTableName);
            // use refresh time to register duns count
            cache.setSnapshotTimestamp(new Date(report.getRefreshTimestamp()));
            dataReportProxy.registerDunsCount(configuration.getCustomerSpace().toString(), DataReportRecord.Level.Upload,
                    uploadId, cache);
        }
    }
}
