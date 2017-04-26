package com.latticeengines.datacloud.dataflow.transformation.stats.report;

import java.util.ArrayList;
import java.util.List;

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
        Node node = addSource(parameters.getBaseTables().get(0));

        return createReportGenerationNode(parameters, node);
    }

    private Node createReportGenerationNode(AccountMasterStatsParameters parameters, Node grouped) {

        List<String> splunkReportColumns = new ArrayList<>();
        List<FieldMetadata> reportOutputColumns = //
                getFinalReportColumns(parameters.getFinalDimensionColumns(), //
                        parameters.getCubeColumnName(), splunkReportColumns);

        Node report = generateFinalReport(grouped, //
                getFieldList(grouped.getSchema()), //
                reportOutputColumns, //
                Fields.RESULTS, //
                splunkReportColumns, //
                parameters);
        return report;
    }

    private Node generateFinalReport(Node grouped, FieldList applyToFieldList, List<FieldMetadata> reportOutputColumns,
            Fields overrideFieldStrategy, List<String> splunkReportColumns, AccountMasterStatsParameters parameters) {

        AMStatsReportFunction.Params functionParam = //
                new AMStatsReportFunction.Params(//
                        getFields(reportOutputColumns), //
                        reportOutputColumns, getTotalKey(), //
                        splunkReportColumns, parameters);

        AMStatsReportFunction reportGenerationFunction = //
                new AMStatsReportFunction(functionParam);

        Node report = grouped.apply(reportGenerationFunction, //
                applyToFieldList, reportOutputColumns, //
                getFieldList(reportOutputColumns), overrideFieldStrategy);

        report = report.addRowID(AccountMasterStatsParameters.PID_KEY);

        report = report.addColumnWithFixedValue(AccountMasterStatsParameters.DATA_CLOUD_VERSION,
                parameters.getDataCloudVersion(), String.class);

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
}
