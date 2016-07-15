package com.latticeengines.network.exposed.metadata;

import java.util.List;

import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.ModelReviewData;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;

public interface RuleResultInterface {

    Boolean createColumnResults(List<ColumnRuleResult> results);

    Boolean createRowResults(List<RowRuleResult> results);

    List<ColumnRuleResult> getColumnResults(String modelId);

    List<RowRuleResult> getRowResults(String modelId);

    ModelReviewData getReviewData(String customerSpace, String modelId, String eventTableName);
}
