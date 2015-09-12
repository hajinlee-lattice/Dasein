package cascading.flow.planner;

public class DataFlowStepJob {
    
    private FlowStepJob<?> flowStepJob;

    public DataFlowStepJob(FlowStepJob<?> flowStepJob) {
        this.flowStepJob = flowStepJob;
    }
    
    public String getJobId() {
        return flowStepJob.internalJobId();
    }
}
