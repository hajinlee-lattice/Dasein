package com.latticeengines.propdata.engine.transformation.service.impl;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.datacloud.dataflow.CharacterizationParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.CollectionDataFlowKeys;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.propdata.core.source.HasSqlPresence;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.CharacterizationSource;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

@Component("characterizationDataFlowService")
public class CharacterizationDataFlowService extends AbstractTransformationDataFlowService {

    private static final Log log = LogFactory.getLog(CharacterizationDataFlowService.class);

    public void executeDataProcessing(Source source, String workflowDir, String sourceVersion, String uid,
            String dataFlowBean, TransformationConfiguration transformationConfiguration) {

        if (StringUtils.isEmpty(dataFlowBean)) {
            throw new LedpException(LedpCode.LEDP_25012,
                    new String[] { source.getSourceName(), "Name of dataFlowBean cannot be null" });
        }

        CharacterizationSource report = (CharacterizationSource) source;
        String flowName = CollectionDataFlowKeys.TRANSFORM_FLOW;
        String targetPath = hdfsPathBuilder.constructWorkFlowDir(source, flowName).append(uid).toString();

        log.info("Creating characterization report @" + sourceVersion);

        Map<String, Table> sourceTables = new HashMap<>();

        // Set up base sources.
        List<String> baseTables = new ArrayList<>();
        Source baseSource = report.getBaseSources()[0];
        String baseSourceName = baseSource.getSourceName();
        log.info("Add base source " + baseSourceName);

        Table baseSourceTable = null;
        String baseSourceVersion = null;
        Long totalRecords = null;
        try {
            baseSourceVersion = hdfsSourceEntityMgr.getCurrentVersion(baseSource);
            baseSourceTable = hdfsSourceEntityMgr.getTableAtVersion(baseSource, baseSourceVersion);
            totalRecords = hdfsSourceEntityMgr.count(baseSource, baseSourceVersion);
            log.info("Select base source " + baseSourceName + "@version " + baseSourceVersion);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25012,
                    new String[] { baseSource.getSourceName(), "is not initialized in HDFS" });
        }

        sourceTables.put(baseSourceName, baseSourceTable);
        baseTables.add(baseSource.getSourceName());

        CharacterizationParameters parameters = new CharacterizationParameters();

        parameters.setTimestamp(new Date());
        parameters.setTimestampField(report.getTimestampField());
        parameters.setBaseTables(baseTables);
        parameters.setHasSqlPresence(source instanceof HasSqlPresence);

        parameters.setVersionKey(report.getVersionKey());
        parameters.setAttrKey(report.getAttrKey());
        parameters.setCategoryKey(report.getCategoryKey());
        parameters.setCountKey(report.getCountKey());
        parameters.setPercentKey(report.getPercentKey());
        parameters.setGroupKeys(Arrays.asList(report.getGroupKeys()));

        HashSet<String> excludeCols = new HashSet<String>();
        String[] excludeAttrs = report.getExcludeAttrs();
        for (int i = 0; i < excludeAttrs.length; i++) {
            excludeCols.add(excludeAttrs[i]);
        }
        List<String> attrs = new ArrayList<String>();
        List<String> categories = new ArrayList<String>();
        List<SourceColumn> sourceColumns = sourceColumnEntityMgr.getSourceColumns(baseSourceName);
        String defaultCategory = Category.DEFAULT.getName();

        for (int i = 0; i < sourceColumns.size(); i++) {
            SourceColumn col = sourceColumns.get(i);
            String attr = col.getColumnName();
            if (excludeCols.contains(attr)) {
                continue;
            }
            List<String> attrCategories = col.getCategoryList();
            String category = (attrCategories.size() == 0) ? defaultCategory : attrCategories.get(0);
            attrs.add(attr);
            categories.add(category);
        }
        parameters.setAttrs(attrs);
        parameters.setCategories(categories);

        parameters.setTotalRecords(totalRecords);
        parameters.setVersion(baseSourceVersion);

        DataFlowContext ctx = dataFlowContext(source, sourceTables, parameters, targetPath);
        Properties jobProperties = ctx.getProperty(DataFlowProperty.JOBPROPERTIES, Properties.class);
        jobProperties.put("mapreduce.job.reduces", String.valueOf(Math.min(cascadingPartitions, 32)));
        ctx.setProperty(DataFlowProperty.ENFORCEGLOBALORDERING, false);
        ctx.setProperty(DataFlowProperty.FLOWNAME, source.getSourceName() + HIPHEN + flowName);

        ctx.setProperty(DataFlowProperty.SOURCETABLES, sourceTables);

        dataTransformationService.executeNamedTransformation(ctx, dataFlowBean);
    }
}
