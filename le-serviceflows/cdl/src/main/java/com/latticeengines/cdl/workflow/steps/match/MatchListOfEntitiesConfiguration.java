package com.latticeengines.cdl.workflow.steps.match;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class MatchListOfEntitiesConfiguration extends MicroserviceStepConfiguration {

    private Map<String, MatchConfiguration> matchConfigurations = new HashMap<>();
    
    public Map<String, MatchConfiguration> getMatchConfigs() {
        return matchConfigurations;
    }

    public void setMatchConfigs(Map<String, MatchConfiguration> matchConfigs) {
        this.matchConfigurations = matchConfigs;
    }

    @Override
    public void setCustomerSpace(CustomerSpace customerSpace) {
        super.setCustomerSpace(customerSpace);
        if (matchConfigurations == null) {
            return;
        }
        matchConfigurations.values().stream().forEach(x -> x.setCustomerSpace(customerSpace));
    }

    @Override
    public void setMicroServiceHostPort(String microServiceHostPort) {
        super.setMicroServiceHostPort(microServiceHostPort);
        if (matchConfigurations == null) {
            return;
        }
        matchConfigurations.values().stream().forEach(x -> x.setMicroServiceHostPort(microServiceHostPort));
    }

    @Override
    public void setInternalResourceHostPort(String internalResourceHostPort) {
        super.setInternalResourceHostPort(internalResourceHostPort);
        if (matchConfigurations == null) {
            return;
        }
        matchConfigurations.values().stream().forEach(x -> x.setInternalResourceHostPort(internalResourceHostPort));
    }

    public static class Builder {
        
        private Map<String, MatchConfiguration.Builder> matchConfigsBuilder = new HashMap<>();
        private CustomerSpace customerSpace;
        
        private MatchConfiguration.Builder getMatchConfigurationBuilderByName(String name) {
            MatchConfiguration.Builder matchConfigurationBldr = matchConfigsBuilder.get(name);
            if (matchConfigurationBldr == null) {
                matchConfigurationBldr = new MatchConfiguration.Builder();
                matchConfigsBuilder.put(name, matchConfigurationBldr);
            }
            return matchConfigurationBldr;
        }
        
        public Builder sourceFile(String name, SourceFile sourceFile) {
            String dataPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString();
            String schemaInterpretation = sourceFile.getSchemaInterpretation().name();
            MatchConfiguration.Builder configBldr = getMatchConfigurationBuilderByName(name);
            configBldr.targetTableName(sourceFile.getSchemaInterpretation().name() + "Staging") //
                .inputDir(String.format("%s/%sStagingNoLatticeId", dataPath, schemaInterpretation)) //
                .outputDir(String.format("%s/%sStaging", dataPath, schemaInterpretation));
            return this;
        }
        
        public Builder customer(CustomerSpace customerSpace) {
            this.customerSpace = customerSpace;
            return this;
        }

        
        public MatchListOfEntitiesConfiguration build() {
            MatchListOfEntitiesConfiguration config = new MatchListOfEntitiesConfiguration();
            
            matchConfigsBuilder.values().forEach(x -> x.customerSpace(customerSpace));
            Map<String, MatchConfiguration> matchConfigs = matchConfigsBuilder.entrySet().stream().collect( //
                    Collectors.toMap(e -> e.getKey(), e -> e.getValue().build()));
            config.setMatchConfigs(matchConfigs);
            return config;
        }

        
    }
}
