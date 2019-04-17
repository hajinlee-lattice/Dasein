package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.IngestedRawSource;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.dataflow.transformation.MostRecentFlow;
import com.latticeengines.domain.exposed.datacloud.dataflow.MostRecentParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.MostRecentConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Table;

@Component(MostRecentFlow.TRANSFORMER_NAME)
public class MostRecentTransformer
        extends AbstractDataflowTransformer<MostRecentConfig, MostRecentParameters> {
    private static final Logger log = LoggerFactory.getLogger(MostRecentTransformer.class);
    private Date latestToArchive;
    private Date earliestToArchive;

    @Autowired
    HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Override
    protected String getDataFlowBeanName() {
        return MostRecentFlow.DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getName() {
        return MostRecentFlow.TRANSFORMER_NAME;
    }


    protected Map<String, Table> setupSourceTables(Map<Source, List<String>> baseSourceVersions) {
        Map<String, Table> sourceTables = new HashMap<>();
        for (Map.Entry<Source, List<String>> entry : baseSourceVersions.entrySet()) {
            Source baseSource = entry.getKey();
            List<String> baseSourceVersion = entry.getValue();
            log.info("Add base source " + baseSource.getSourceName());
            addSource(sourceTables, baseSource, baseSourceVersion);
        }
        return sourceTables;
    }

    @Override
    protected Class<MostRecentParameters> getDataFlowParametersClass() {
        return MostRecentParameters.class;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return MostRecentConfig.class;
    }


    protected boolean addSource(Map<String, Table> sourceTables, Source source, List<String> versions) {
        String sourceName = source.getSourceName();
        Table sourceTable;
        try {
            if (source instanceof IngestedRawSource) {
                sourceTable = hdfsSourceEntityMgr.getCollectedTableSince((IngestedRawSource) source, earliestToArchive);
            } else {
                throw new RuntimeException("Source is not instance of IngestedRawSource");
            }
        } catch (Exception e) {
            log.error("Source " + sourceName + " is not initiated in HDFS", e);
            return false;
        }
        sourceTables.put(sourceName, sourceTable);
        return true;
    }

    @Override
    protected void updateParameters(MostRecentParameters parameters, Source[] baseSource, Source target,
            MostRecentConfig config, List<String> baseVersions) {
        try {
            latestToArchive = HdfsPathBuilder.dateFormat.parse(baseVersions.get(0));
            earliestToArchive = new Date(latestToArchive.getTime() - config.getPeriodToKeep());
            parameters.setEarliest(earliestToArchive);
            parameters.setDomainField(config.getDomainField());
            parameters.setTimestampField(config.getTimestampField());
            parameters.setGroupbyFields(config.getGroupbyFields());
        } catch (Exception e) {
            log.error("Failed to update parameters", e);
        }
    }
}
