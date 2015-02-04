package com.latticeengines.scoringharness.marketoharness;

import org.springframework.context.ApplicationContext;

import com.latticeengines.scoringharness.OutputFileWriter;
import com.latticeengines.scoringharness.operationmodel.Operation;
import com.latticeengines.scoringharness.operationmodel.OperationFactory;
import com.latticeengines.scoringharness.operationmodel.OperationSpec;
import com.latticeengines.scoringharness.operationmodel.ReadLeadScoreOperationSpec;
import com.latticeengines.scoringharness.operationmodel.WriteLeadOperationSpec;

public class MarketoOperationFactory extends OperationFactory {

    private ApplicationContext context;
    private OutputFileWriter output;

    public MarketoOperationFactory(ApplicationContext context, OutputFileWriter output) {
        // TODO Is there any way to avoid passing the context around here?
        this.context = context;
        this.output = output;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends OperationSpec> Operation<T> getOperation(T spec) {
        Operation<T> operation;
        if (spec.getClass() == ReadLeadScoreOperationSpec.class) {
            operation = (Operation<T>) context.getBean(ReadLeadScoreFromMarketoOperation.class);
        } else if (spec.getClass() == WriteLeadOperationSpec.class) {
            operation = (Operation<T>) context.getBean(WriteLeadToMarketoOperation.class);
        } else {
            throw new RuntimeException("Unknown operation spec type " + spec.getClass());
        }
        operation.setOutput(output);
        operation.setSpec(spec);
        return operation;
    }
}
