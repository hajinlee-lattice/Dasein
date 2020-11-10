package com.latticeengines.cdl.workflow.steps.importdata;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.ImportFileSignature;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportDataFeedTaskConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.dcp.InputPresenceConfig;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.dcp.InputPresenceJob;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AnalyzeImportFile extends RunSparkJob<ImportDataFeedTaskConfiguration, InputPresenceConfig> {

    private static final Logger log = LoggerFactory.getLogger(AnalyzeImportFile.class);

    @Inject
    private EaiJobDetailProxy eaiJobDetailProxy;

    @Override
    protected Class<? extends AbstractSparkJob<InputPresenceConfig>> getJobClz() {
        return InputPresenceJob.class;
    }

    @Override
    protected InputPresenceConfig configureJob(ImportDataFeedTaskConfiguration stepConfiguration) {
        String applicationId = getOutputValue(WorkflowContextConstants.Outputs.EAI_JOB_APPLICATION_ID);
        if (applicationId == null) {
            log.error("Cannot find EAI import job application!");
            return null;
        }
        EaiImportJobDetail importJobDetail = eaiJobDetailProxy.getImportJobDetailByAppId(applicationId);
        if (importJobDetail == null || CollectionUtils.isEmpty(importJobDetail.getPathDetail())) {
            log.error("EAI import job {} not exists, or cannot find import data.", applicationId);
            return null;
        }
        List<DataUnit> dataUnits = importJobDetail.getPathDetail().stream().map(HdfsDataUnit::fromPath).collect(Collectors.toList());
        InputPresenceConfig config = new InputPresenceConfig();
        config.setInput(dataUnits);
        config.setInputNames(ImmutableSet.of(InterfaceName.DUNS.name(), InterfaceName.Website.name(),
                InterfaceName.CompanyName.name(), InterfaceName.Email.name()));
        config.setExcludeEmpty(Boolean.TRUE);
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        Map<String, Long> map = JsonUtils.convertMap(JsonUtils.deserialize(result.getOutput(), Map.class),
                String.class, Long.class);
        ImportFileSignature fileSignature = new ImportFileSignature();
        if (MapUtils.isNotEmpty(map)) {
            fileSignature.setHasDUNS(map.containsKey(InterfaceName.DUNS.name()) && map.get(InterfaceName.DUNS.name()) > 0L);
            fileSignature.setHasDomain((map.containsKey(InterfaceName.Website.name()) && map.get(InterfaceName.Website.name()) > 0L)
                    || (map.containsKey(InterfaceName.Email.name())) && map.get(InterfaceName.Email.name()) > 0L);
            fileSignature.setHasCompanyName(map.containsKey(InterfaceName.CompanyName.name()) && map.get(InterfaceName.CompanyName.name()) > 0L);
        }
        putObjectInContext(IMPORT_FILE_SIGNATURE, fileSignature);
    }
}
