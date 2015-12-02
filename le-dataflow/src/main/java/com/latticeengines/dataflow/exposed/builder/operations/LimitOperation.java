package com.latticeengines.dataflow.exposed.builder.operations;

import java.beans.ConstructorProperties;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;

import cascading.flow.FlowProcess;
import cascading.management.annotation.Property;
import cascading.management.annotation.PropertyDescription;
import cascading.management.annotation.Visibility;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;

public class LimitOperation extends Operation {
    public LimitOperation(String prior, int count, CascadingDataFlowBuilder builder) {
        super(builder);

        if (!builder.enforceGlobalOrdering()) {
            throw new RuntimeException(
                    "Builder must enforce global ordering in order to perform a deterministic limit operation");
        }

        Pipe priorPipe = getPipe(prior);
        this.pipe = new Each(priorPipe, new Limit(count));
        this.metadata = getMetadata(prior);
    }

    /**
     * Limit with bug fix to avoid divide by zero error.
     */
    public static class Limit extends BaseOperation<Limit.Context> implements Filter<Limit.Context> {
        private long limit = 0;

        public static class Context {
            public long limit = 0;
            public long count = 0;

            public boolean increment() {
                if (limit == count)
                    return true;

                count++;

                return false;
            }
        }

        /**
         * Creates a new Limit class that only allows limit number of Tuple
         * instances to pass.
         *
         * @param limit
         *            the number of tuples to let pass
         */
        @ConstructorProperties({ "limit" })
        public Limit(long limit) {
            this.limit = limit;
        }

        @Property(name = "limit", visibility = Visibility.PUBLIC)
        @PropertyDescription("The upper limit.")
        public long getLimit() {
            return limit;
        }

        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
            super.prepare(flowProcess, operationCall);

            Context context = new Context();

            operationCall.setContext(context);

            /**
             * Avoid the divide by zero error
             */
            int numTasks = flowProcess.getNumProcessSlices() <= 0 ? 1 : flowProcess.getNumProcessSlices();
            int taskNum = flowProcess.getCurrentSliceNum();

            context.limit = (long) Math.floor((double) limit / (double) numTasks);

            long remainingLimit = limit % numTasks;

            // evenly divide limits across tasks
            context.limit += taskNum < remainingLimit ? 1 : 0;
        }

        @Override
        public boolean isRemove(FlowProcess flowProcess, FilterCall<Context> filterCall) {
            return filterCall.getContext().increment();
        }

        @Override
        public boolean equals(Object object) {
            if (this == object)
                return true;
            if (!(object instanceof Limit))
                return false;
            if (!super.equals(object))
                return false;

            Limit limit1 = (Limit) object;

            if (limit != limit1.limit)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (int) (limit ^ limit >>> 32);
            return result;
        }
    }

}
