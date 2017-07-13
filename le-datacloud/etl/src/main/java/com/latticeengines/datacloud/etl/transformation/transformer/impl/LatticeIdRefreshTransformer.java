package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.dataflow.transformation.LatticeIdRefreshFlow;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.entitymgr.LatticeIdStrategyEntityMgr;
import com.latticeengines.domain.exposed.datacloud.dataflow.LatticeIdRefreshFlowParameter;
import com.latticeengines.domain.exposed.datacloud.manage.LatticeIdStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.LatticeIdRefreshConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component("latticeIdRefreshTransformer")
public class LatticeIdRefreshTransformer
        extends AbstractDataflowTransformer<LatticeIdRefreshConfig, LatticeIdRefreshFlowParameter> {

    private static final Logger log = LoggerFactory.getLogger(LatticeIdRefreshTransformer.class);

    @Autowired
    private LatticeIdStrategyEntityMgr latticeIdStrategyEntityMgr;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private SourceService sourceService;

    private static String transfomerName = "latticeIdRefreshTransformer";

    private static String dataFlowBeanName = "latticeIdRefreshFlow";

    @Override
    protected void updateParameters(LatticeIdRefreshFlowParameter parameters, Source[] baseTemplates,
            Source targetTemplate, LatticeIdRefreshConfig config, List<String> baseVersions) {
        parameters.setStrategy(latticeIdStrategyEntityMgr.getStrategyByName(config.getStrategy()));
        if (config.getCurrentCount() == null) {
            // 0th base source should be source of LatticeId
            Long currentCount = hdfsSourceEntityMgr.count(baseTemplates[0],
                    hdfsSourceEntityMgr.getCurrentVersion(baseTemplates[0]));
            if (currentCount == null) {
                throw new RuntimeException("Fail to get current count of LatticeId source");
            }
            parameters.setCurrentCount(currentCount);
        } else {
            parameters.setCurrentCount(config.getCurrentCount());
        }
    }

    @Override
    protected boolean validateConfig(LatticeIdRefreshConfig config, List<String> sourceNames) {
        if (StringUtils.isEmpty(config.getStrategy())) {
            log.error("LatticeIdStrategy name is not provided");
            return false;
        }
        if (CollectionUtils.isEmpty(sourceNames) || sourceNames.size() != 2) {
            log.error("Number of base sources must be 2");
            return false;
        }
        return true;
    }

    @Override
    protected void initBaseSources(LatticeIdRefreshConfig config, List<String> sourceNames) {
        LatticeIdStrategy strategy = latticeIdStrategyEntityMgr.getStrategyByName(config.getStrategy());
        if (strategy == null) {
            throw new RuntimeException("Fail to find LatticeIdStrategy with name " + config.getStrategy());
        }
        // 0th base source should be source of LatticeId
        initLatticeId(strategy, sourceNames.get(0));
    }

    private void initLatticeId(LatticeIdStrategy strategy, String sourceName) {
        if (sourceService.findBySourceName(sourceName) == null) {
            sourceService.createSource(sourceName);
        } else {
            return;
        }
        List<Pair<String, Class<?>>> columns = prepareSchema(strategy);
        Object[][] data = new Object[][] {};
        String version = HdfsPathBuilder.dateFormat.format(DateUtils.addDays(new Date(), -1));
        uploadAvro(sourceName, version, columns, data);
    }

    private List<Pair<String, Class<?>>> prepareSchema(LatticeIdStrategy strategy) {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        for (List<String> attrs : strategy.getKeyMap().values()) {
            for (String attr : attrs) {
                columns.add(Pair.of(attr, String.class));
            }
        }
        columns.add(Pair.of(LatticeIdRefreshFlow.STATUS_FIELD, String.class));
        columns.add(Pair.of(LatticeIdRefreshFlow.TIMESTAMP_FIELD, Long.class));
        switch (strategy.getIdType()) {
        case LONG:
            columns.add(Pair.of(strategy.getIdName(), Long.class));
            columns.add(Pair.of(LatticeIdRefreshFlow.REDIRECT_FROM_FIELD, Long.class));
            break;
        case UUID:
            columns.add(Pair.of(LatticeIdRefreshFlow.REDIRECT_FROM_FIELD, String.class));
            break;
        default:
            throw new RuntimeException(
                    String.format("IdType %s in LatticeIdStrategy is not supported", strategy.getIdType().name()));
        }
        return columns;
    }

    private void uploadAvro(String sourceName, String verson,
            List<Pair<String, Class<?>>> schema, Object[][] data) {
        String targetDir = hdfsPathBuilder.constructSnapshotDir(sourceName, verson).toString();
        String successPath = hdfsPathBuilder.constructSnapshotDir(sourceName, verson).append("_SUCCESS")
                .toString();
        try {
            AvroUtils.createAvroFileByData(yarnConfiguration, schema, data, targetDir, "part-00000.avro");
            InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, successPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        hdfsSourceEntityMgr.setCurrentVersion(sourceName, verson);
    }

    @Override
    protected String getDataFlowBeanName() {
        return dataFlowBeanName;
    }

    @Override
    public String getName() {
        return transfomerName;
    }

    @Override
    protected Class<LatticeIdRefreshFlowParameter> getDataFlowParametersClass() {
        return LatticeIdRefreshFlowParameter.class;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return LatticeIdRefreshConfig.class;
    }

}
