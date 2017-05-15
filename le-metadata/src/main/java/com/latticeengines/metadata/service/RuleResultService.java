package com.latticeengines.metadata.service;

import java.util.List;

import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;

public interface RuleResultService {

    void createColumnResults(List<ColumnRuleResult> columnResults);

    void createRowResults(List<RowRuleResult> rowResults);

    void deleteColumnResults(List<ColumnRuleResult> columnResults);

    void deleteRowResults(List<RowRuleResult> rowResults);

    List<ColumnRuleResult> findColumnResults(String modelId);

    List<RowRuleResult> findRowResults(String modelId);
}
