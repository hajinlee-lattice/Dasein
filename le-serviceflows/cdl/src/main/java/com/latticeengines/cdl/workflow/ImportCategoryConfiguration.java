package com.latticeengines.cdl.workflow;

public class ImportCategoryConfiguration extends ImportListOfEntitiesConfiguration {
    
    public static class Builder extends ImportListOfEntitiesConfiguration.Builder {

        @SuppressWarnings("unchecked")
        @Override
        public <T extends ImportListOfEntitiesConfiguration> T getConfiguration() {
            return (T) new ImportCategoryConfiguration();
        }
    }
}
