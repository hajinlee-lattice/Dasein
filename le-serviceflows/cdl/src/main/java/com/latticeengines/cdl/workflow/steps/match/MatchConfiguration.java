package com.latticeengines.cdl.workflow.steps.match;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.IOBufferType;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class MatchConfiguration extends MicroserviceStepConfiguration {
    
    private String targetTableName;
    
    private MatchInput matchInput;

    public MatchInput getMatchInput() {
        return matchInput;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public static class Builder {
        private String targetTableName;
        private String inputDir;
        private CustomerSpace customerSpace;
        private String outputDir;
        
        private MatchInput createMatchInput(String inputDir, CustomerSpace customerSpace) {
            MatchInput input = new MatchInput();
            input.setDataCloudVersion("2.0.1");
            input.setDecisionGraph("DragonClaw");
            input.setUseDnBCache(true);
            input.setTenant(new Tenant(customerSpace.toString()));
            AvroInputBuffer buffer = new AvroInputBuffer();
            buffer.setAvroDir(inputDir);
            input.setInputBuffer(buffer);
            input.setMatchResultPath(outputDir);
            input.setOutputBufferType(IOBufferType.AVRO);
            input.setPredefinedSelection(Predefined.ID);
            
            return input;
        }
        
        public MatchConfiguration build() {
            MatchConfiguration matchConfig = new MatchConfiguration();
            matchConfig.targetTableName = targetTableName;
            matchConfig.matchInput = createMatchInput(inputDir, customerSpace);
            return matchConfig;
        }
        
        public Builder customerSpace(CustomerSpace customerSpace) {
            this.customerSpace = customerSpace;
            return this;
        }
        
        public Builder targetTableName(String targetTableName) {
            this.targetTableName = targetTableName;
            return this;
        }
        
        public Builder inputDir(String inputDir) {
            this.inputDir = inputDir;
            return this;
        }

        public Builder outputDir(String outputDir) {
            this.outputDir = outputDir;
            return this;
        }
    }


}
