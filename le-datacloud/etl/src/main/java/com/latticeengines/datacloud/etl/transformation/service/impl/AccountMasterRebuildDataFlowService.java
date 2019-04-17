package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.DomainBased;
import com.latticeengines.datacloud.core.source.DunsBased;
import com.latticeengines.datacloud.core.source.HasSqlPresence;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMaster;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeed;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterRebuildParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterSourceParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.CollectionDataFlowKeys;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;

@Component("accountMasterRebuildDataFlowService")
public class AccountMasterRebuildDataFlowService extends AbstractTransformationDataFlowService
                                                   implements ApplicationContextAware {

    private static final Logger log = LoggerFactory.getLogger(AccountMasterRebuildDataFlowService.class);

    private ApplicationContext applicationContext;

    public void executeDataProcessing(Source source, String workflowDir, String sourceVersion, String uid,
                                      String dataFlowBean, TransformationConfiguration transformationConfiguration) {

        if (StringUtils.isEmpty(dataFlowBean)) {
            throw new LedpException(LedpCode.LEDP_25012,
                    new String[] { source.getSourceName(), "Name of dataFlowBean cannot be null" });
        }

        AccountMaster accountMaster = (AccountMaster) source;
        String flowName = CollectionDataFlowKeys.TRANSFORM_FLOW;
        String targetPath = hdfsPathBuilder.constructWorkFlowDir(source, flowName).append(uid).toString();

        log.info("Creating AccountMaster @" + sourceVersion);

        Map<String, Source> sources = new HashMap<>();
        Map<String, Table> sourceTables = new HashMap<>();
        Map<String, HashSet<String>> sourceColumnMap = new HashMap<String, HashSet<String>>();

        // Set up base sources.
        List<String> baseTables = new ArrayList<>();
        for (Source baseSource : accountMaster.getBaseSources()) {
            log.info("Add base source " + baseSource.getSourceName());
            addSource(sources, sourceTables, sourceColumnMap, baseSource);
            baseTables.add(baseSource.getSourceName());
        }

        AccountMasterRebuildParameters parameters = new AccountMasterRebuildParameters();

        addSources(parameters, accountMaster, sources, sourceTables, sourceColumnMap);

        parameters.setTimestamp(new Date());
        parameters.setTimestampField(accountMaster.getTimestampField());
        parameters.setBaseTables(baseTables);
        parameters.setJoinFields(source.getPrimaryKey());
        parameters.setHasSqlPresence(source instanceof HasSqlPresence);

        DataFlowContext ctx = dataFlowContext(source, sourceTables, parameters, targetPath);
        ctx.setProperty(DataFlowProperty.FLOWNAME, source.getSourceName() + HIPHEN + flowName);
        ctx.setProperty(DataFlowProperty.SOURCETABLES, sourceTables);

        dataTransformationService.executeNamedTransformation(ctx, dataFlowBean);
    }

    private boolean addSource(Map<String, Source> sources, Map<String, Table> sourceTables, Map<String, HashSet<String>> columnMap,
            Source source) {

        String sourceName = source.getSourceName();
        log.info("Add source " + sourceName);

        HashSet<String> columnSet = new HashSet<String>();
        List<SourceColumn> sourceColumns = sourceColumnEntityMgr.getSourceColumns(sourceName);
        for (SourceColumn column : sourceColumns) {
            columnSet.add(column.getColumnName());
            log.info("Column " + column.getColumnName());
        }
        columnMap.put(sourceName, columnSet);

        Table sourceTable = null;
        try {
            String version = hdfsSourceEntityMgr.getCurrentVersion(source);
            sourceTable = hdfsSourceEntityMgr.getTableAtVersion(source, version);
            log.info("Select source " + sourceName + "@version " + version);


        } catch (Exception e) {
            log.info("Source " + sourceName + " is not initiated in HDFS");
            e.printStackTrace();
            return false;
        }

        sources.put(sourceName, source);
        sourceTables.put(sourceName, sourceTable);
        return true;
    }

    @SuppressWarnings("unchecked")
    private void addSources(AccountMasterRebuildParameters rebuildParameters, AccountMaster accountMaster, Map<String, Source> sources,
            Map<String, Table> sourceTables, Map<String, HashSet<String>> sourceColumnMap) {

        AccountMasterSeed seed = (AccountMasterSeed) accountMaster.getBaseSources()[0];

        String domainKey = seed.getDomainField();
        String dunsKey = seed.getDunsField();

        List<SourceColumn> sourceColumns = sourceColumnEntityMgr.getSourceColumns(accountMaster.getSourceName());

        Map<String, AccountMasterSourceParameters> sourceParametersMap =  new HashMap<String, AccountMasterSourceParameters>();
        HashSet<String> blackList = new HashSet<String>();

        for (SourceColumn column : sourceColumns) {

            log.info("Processing column " + column.getColumnName() + " " + column.getBaseSource() + " " + column.getArguments());

            String sourceName = column.getBaseSource();
            if ((sourceName == null) || sourceName.trim().isEmpty()) {
                log.info("Skip column without valid base source " + sourceName + " " + column.getColumnName() + " " + column.getArguments());
                continue;
            }

            if (sourceName.equals("AccountMasterSeed")) {
                log.info("All columns from AccountMasterSeed will be added");
                continue;
            }

            String baseColumnName = null;
            try {
                Map<String, String> props = JsonUtils.deserialize(column.getArguments(), Map.class);
                baseColumnName = props.get("BaseSourceColumn");
            } catch (Exception e) {
                baseColumnName = null;
            }
            if ((baseColumnName == null) || baseColumnName.trim().isEmpty()) {
                log.info("Skip column without valid base column" + sourceName + " " + column.getColumnName() + " " + column.getArguments());
                continue;
            }

            Source source = sources.get(sourceName);
            if (source == null) {
                if (!blackList.contains(sourceName)) {
                    source = loadSource(sourceName);
                    if (source == null) {
                        log.info("Source not found " + sourceName);
                        blackList.add(sourceName);
                    } else if (!addSource(sources, sourceTables, sourceColumnMap, source)) {
                        blackList.add(sourceName);
                        source = null;
                    }
               }

            }

            HashSet<String> columnSet = sourceColumnMap.get(sourceName);
            if (columnSet == null) {
                continue;
            }
            if (!columnSet.contains(baseColumnName)) {
                log.info("Skip column(base column not in source)" + sourceName + " " + column.getColumnName() + " " + column.getArguments());
                continue;
            }

            if (source == null) {
                continue;
            }

            AccountMasterSourceParameters sourceParameters = sourceParametersMap.get(sourceName);
            if (sourceParameters == null) {
                sourceParameters = buildSourceParameters(sourceName, source);
                if (sourceParameters != null) {
                    sourceParametersMap.put(sourceName, sourceParameters);
                } else {
                    continue;
                }
            }


            sourceParameters.addAttribute(baseColumnName, column.getColumnName());
        }

        logSourceInfo(sourceParametersMap);

        ArrayList<String> id = new ArrayList<String>();
        id.add(seed.getPrimaryKey()[0]);
        rebuildParameters.setId(id);
        rebuildParameters.setDomainKey(domainKey);
        rebuildParameters.setDunsKey(dunsKey);
        rebuildParameters.setSeedFields(seed.getRetainFields());
        rebuildParameters.setSourceParameters(new ArrayList<AccountMasterSourceParameters>(sourceParametersMap.values()));
        rebuildParameters.setSourceColumns(sourceColumns);
        rebuildParameters.setLastSource("DnBCacheSeed");
    }

    private void logSourceInfo(Map<String, AccountMasterSourceParameters> sourceMap) {

        log.info("Source conversion info");
        for (String key : sourceMap.keySet()) {
            AccountMasterSourceParameters sourceParameters = sourceMap.get(key);
            log.info("Source Name: " + sourceParameters.getSourceName());
            log.info("Source sourceType: " + sourceParameters.getSourceType());
            log.info("Source Join Key: " + sourceParameters.getJoinKey());
            log.info("Source Second Key: " + sourceParameters.getSecondKey());
            log.info("Source column mappings: ");
            List<String> sourceAttrs = sourceParameters.getSourceAttrs();
            List<String> outputAttrs = sourceParameters.getOutputAttrs();

            for (int i = 0; i < sourceAttrs.size(); i++) {
                 log.info("   Source: " + sourceAttrs.get(i) + " Output: " + outputAttrs.get(i));
            }
        }
    }


    private AccountMasterSourceParameters buildSourceParameters(String sourceName, Source source) {

        if (!(source instanceof DomainBased) && !(source instanceof DunsBased)) {
            log.info("Source is neither domain based or dun based " + sourceName);
            return null;
        }

        AccountMasterSourceParameters sourceParameters = new AccountMasterSourceParameters();
        sourceParameters.setSourceName(sourceName);

        if (source instanceof DunsBased) {
            sourceParameters.setSourceType(AccountMasterSourceParameters.DunsBased);
            sourceParameters.setJoinKey(((DunsBased)source).getDunsField());
            if (source instanceof DomainBased) {
                sourceParameters.setSecondKey(((DomainBased)source).getDomainField());
                log.info(sourceName + " is both domain and duns based. Treat it as duns based");
            }
            log.info("Add Duns based source " + sourceName + " " + ((DunsBased)source).getDunsField());
        } else if (source instanceof DomainBased) {
            sourceParameters.setSourceType(AccountMasterSourceParameters.DomainBased);
            sourceParameters.setJoinKey(((DomainBased)source).getDomainField());
            log.info("Add Domain based source " + sourceName + " " + ((DomainBased)source).getDomainField());
        } else {
            log.info(sourceName + " is neither DUNS based nor Domain based, skip.");
        }

        return sourceParameters;
    }

    private Source loadSource(String sourceName) {
        Source source = null;
        try {
            char beanName[] = sourceName.toCharArray();
            beanName[0] = Character.toLowerCase(beanName[0]);
            if (sourceName.equals("HGDataPivoted") || sourceName.equals("HGDataTechIndicators")) {
                beanName[1] = Character.toLowerCase(beanName[1]);
            } if (sourceName.equals("HPANewPivoted")) {
                beanName[1] = Character.toLowerCase(beanName[1]);
                beanName[2] = Character.toLowerCase(beanName[2]);
            }
            source = (Source)applicationContext.getBean(new String(beanName));
        } catch (Exception e) {
            log.info("Failed to load source " + sourceName);
        }
        return source;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
