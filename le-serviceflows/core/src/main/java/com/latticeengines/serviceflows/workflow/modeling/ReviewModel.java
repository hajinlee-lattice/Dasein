package com.latticeengines.serviceflows.workflow.modeling;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.serviceflows.core.steps.ModelStepConfiguration;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Splitter;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modelreview.BaseRuleResult;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.serviceflows.workflow.core.ModelingServiceExecutor;

@Component("reviewModel")
public class ReviewModel extends BaseModelStep<ModelStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ReviewModel.class);

    @Override
    public void execute() {
        log.info("Inside ReviewModel execute()");

        Map<String, List<ColumnRuleResult>> eventToColumnResults = new HashMap<>();
        Map<String, List<RowRuleResult>> eventToRowResults = new HashMap<>();

        Table eventTable = getEventTable();
        List<Attribute> events = eventTable.getAttributes(LogicalDataType.Event);
        for (Attribute event : events) {
            try {
                ModelingServiceExecutor modelExecutor = createModelingServiceExecutor(eventTable, event);
                modelExecutor.review();

                String dataRulesHdfsPath = String.format("%s/%s/data/%s/datarules/",
                        configuration.getModelingServiceHdfsBaseDir(), configuration.getCustomerSpace().toString(),
                        getMetadataTableFolderName(eventTable, event));
                log.info("Retrieving datarule results from:" + dataRulesHdfsPath);
                List<ColumnRuleResult> columnResults = new ArrayList<>();
                List<RowRuleResult> rowResults = new ArrayList<>();
                try {
                    List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, dataRulesHdfsPath);
                    for (String filePath : files) {
                        if (filePath.endsWith("_ColumnRule.avro")) {
                            columnResults.add(getColumnResult(filePath));
                        } else {
                            rowResults.add(getRowResult(filePath));
                        }
                    }
                } catch (IOException e) {
                    throw new LedpException(LedpCode.LEDP_28027, e, new String[] { dataRulesHdfsPath });
                }

                eventToColumnResults.put(event.getName(), columnResults);
                eventToRowResults.put(event.getName(), rowResults);
            } catch (LedpException e) {
                throw e;
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_28007, e, new String[] { eventTable.getName() });
            }
        }
        putObjectInContext(COLUMN_RULE_RESULTS, eventToColumnResults);
        putObjectInContext(ROW_RULE_RESULTS, eventToRowResults);
    }

    ColumnRuleResult getColumnResult(String filePath) {
        ColumnRuleResult columnResult = new ColumnRuleResult();
        setBaseRuleAttr(filePath, columnResult);

        List<String> columnNames = new ArrayList<>();
        columnResult.setFlaggedColumnNames(columnNames);

        List<GenericRecord> avroRecords = AvroUtils.getDataFromGlob(yarnConfiguration, filePath);
        for (GenericRecord avroRecord : avroRecords) {
            Object columnNameObj = avroRecord.get("ColumnName");
            columnNames.add(String.valueOf(columnNameObj));
        }

        return columnResult;
    }

    RowRuleResult getRowResult(String filePath) {
        RowRuleResult rowResult = new RowRuleResult();
        setBaseRuleAttr(filePath, rowResult);

        Map<String, List<String>> flaggedRowIdAndColumnNames = new HashMap<>();
        Map<String, Boolean> flaggedRowIdAndPositiveEvent = new HashMap<>();
        rowResult.setFlaggedRowIdAndColumnNames(flaggedRowIdAndColumnNames);
        rowResult.setFlaggedRowIdAndPositiveEvent(flaggedRowIdAndPositiveEvent);

        List<GenericRecord> avroRecords = AvroUtils.getDataFromGlob(yarnConfiguration, filePath);
        for (GenericRecord avroRecord : avroRecords) {
            Object rowIdObj = avroRecord.get("itemid");
            String rowId = String.valueOf(rowIdObj);
            Object isPositiveEventObj = avroRecord.get("isPositiveEvent");
            flaggedRowIdAndPositiveEvent.put(rowId, (Boolean) isPositiveEventObj);
            Object columnNamesObj = avroRecord.get("columns");
            String columnNamesStr = String.valueOf(columnNamesObj);
            List<String> columnNames = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(columnNamesStr);
            flaggedRowIdAndColumnNames.put(rowId, columnNames);
        }

        return rowResult;
    }

    void setBaseRuleAttr(String filePath, BaseRuleResult result) {
        String ruleResultFileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        String dataRuleName = ruleResultFileName.substring(0, ruleResultFileName.indexOf("_"));
        result.setDataRuleName(dataRuleName);
        // modelId and tenant will be set on tenant in persistDataRules step
    }

}
