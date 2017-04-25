package com.latticeengines.datacloud.dataflow.transformation.stats.report;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.AMStatsFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsReportFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component("amStatsReportFlow")
public class AMStatsReportFlow extends AMStatsFlowBase {

    private static Log log = LogFactory.getLog(AMStatsReportFlow.class);

    @Override
    public Node construct(AccountMasterStatsParameters parameters) {
        String dataCloudVersion = parameters.getDataCloudVersion();

        Node node = addSource(parameters.getBaseTables().get(0));

        Map<String, List<String>> dimensionDefinitionMap = parameters.getDimensionDefinitionMap();

        Fields groupByFields = new Fields();

        String[] dimensionIdFieldNames = new String[dimensionDefinitionMap.keySet().size()];
        int i = 0;
        for (String dimensionKey : dimensionDefinitionMap.keySet()) {
            groupByFields = groupByFields.append(new Fields(dimensionKey));
            dimensionIdFieldNames[i++] = dimensionKey;
        }

        List<FieldMetadata> fms = node.getSchema();
        node = createReportGenerationNode(parameters, dimensionIdFieldNames, fms, node, dataCloudVersion);

        return node;
    }

    private Node createReportGenerationNode(AccountMasterStatsParameters parameters, String[] dimensionIdFieldNames,
            List<FieldMetadata> fms, Node grouped, String dataCloudVersion) {
        List<String> splunkReportColumns = new ArrayList<>();
        List<FieldMetadata> reportOutputColumns = getFinalReportColumns(parameters.getFinalDimensionColumns(),
                parameters.getCubeColumnName(), splunkReportColumns);

        List<FieldMetadata> reportOutputColumnsFms = new ArrayList<>();
        reportOutputColumnsFms.addAll(reportOutputColumns);

        Node report = generateFinalReport(grouped, //
                getFieldList(fms), reportOutputColumns, //
                getFieldList(reportOutputColumnsFms), Fields.RESULTS, //
                parameters.getCubeColumnName(), parameters.getRootIdsForNonRequiredDimensions(), //
                splunkReportColumns, parameters.getColumnsForStatsCalculation(), //
                parameters.getColumnIdsForStatsCalculation(), dataCloudVersion);
        return report;
    }

    private List<FieldMetadata> getFinalReportColumns(List<String> finalDimensionsList, String encodedCubeColumnName,
            List<String> splunkReportColumns) {
        List<FieldMetadata> finalReportColumns = new ArrayList<>();

        int index = 0;
        for (String dimensionKey : finalDimensionsList) {
            finalReportColumns.add(new FieldMetadata(dimensionKey, Long.class));
            log.info("Final report column " + index + " " + dimensionKey);
            index++;
        }

        finalReportColumns.add(new FieldMetadata(encodedCubeColumnName, String.class));
        finalReportColumns.add(new FieldMetadata(AccountMasterStatsParameters.GROUP_TOTAL_KEY, Long.class));

        splunkReportColumns.add(AccountMasterStatsParameters.ATTR_COUNT_1_KEY);
        finalReportColumns.add(new FieldMetadata(AccountMasterStatsParameters.ATTR_COUNT_1_KEY, String.class));
        splunkReportColumns.add(AccountMasterStatsParameters.ATTR_COUNT_2_KEY);
        finalReportColumns.add(new FieldMetadata(AccountMasterStatsParameters.ATTR_COUNT_2_KEY, String.class));
        splunkReportColumns.add(AccountMasterStatsParameters.ATTR_COUNT_3_KEY);
        finalReportColumns.add(new FieldMetadata(AccountMasterStatsParameters.ATTR_COUNT_3_KEY, String.class));
        splunkReportColumns.add(AccountMasterStatsParameters.ATTR_COUNT_4_KEY);
        finalReportColumns.add(new FieldMetadata(AccountMasterStatsParameters.ATTR_COUNT_4_KEY, String.class));
        return finalReportColumns;
    }

    private Node generateFinalReport(Node grouped, FieldList applyToFieldList, List<FieldMetadata> reportOutputColumns,
            FieldList reportOutputColumnFieldList, Fields overrideFieldStrategy, String cubeColumnName,
            Map<String, Long> rootIdsForNonRequiredDimensions, List<String> splunkReportColumns,
            List<String> columnsForStatsCalculation, List<Integer> columnIdsForStatsCalculation,
            String dataCloudVersion) {

        String[] reportOutputColumnFields = reportOutputColumnFieldList.getFields();

        Fields reportOutputColumnFieldsDeclaration = new Fields(reportOutputColumnFields);

        AMStatsReportFunction.Params functionParam = //
                new AMStatsReportFunction.Params(//
                        reportOutputColumnFieldsDeclaration, reportOutputColumns, cubeColumnName, //
                        getTotalKey(), splunkReportColumns, rootIdsForNonRequiredDimensions, //
                        columnsForStatsCalculation, columnIdsForStatsCalculation, //
                        AccountMasterStatsParameters.DIMENSION_COLUMN_PREPOSTFIX, //
                        AccountMasterStatsParameters.GROUP_TOTAL_KEY);

        AMStatsReportFunction reportGenerationFunction = new AMStatsReportFunction(functionParam);
        Node report = grouped.apply(reportGenerationFunction, applyToFieldList, reportOutputColumns,
                reportOutputColumnFieldList, overrideFieldStrategy);

        report = report.addRowID(AccountMasterStatsParameters.PID_KEY);
        report = report.addColumnWithFixedValue(AccountMasterStatsParameters.DATA_CLOUD_VERSION, dataCloudVersion,
                String.class);

        return report;
    }
}
