package com.latticeengines.propdata.dataflow.refresh;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.propdata.dataflow.AccountMasterReduceParameters;


@Component("accountMasterReduceFlow")
public class AccountMasterReduceFlow extends TypesafeDataFlowBuilder<AccountMasterReduceParameters> {
    private static final Log log = LogFactory.getLog(AccountMasterReduceFlow.class);

    @Override
    public Node construct(AccountMasterReduceParameters parameters) {

        log.info("Add account master as base");

        Node accountMaster = addSource(parameters.getBaseTables().get(0));

        Node filtered = accountMaster.filter("Domain != null", new FieldList("Domain"));

        Node reduced = filtered.groupByAndLimit(new FieldList("Domain"), 1);

        Node stamped = reduced.addTimestamp(parameters.getTimestampField());

        return stamped;
    }
}
