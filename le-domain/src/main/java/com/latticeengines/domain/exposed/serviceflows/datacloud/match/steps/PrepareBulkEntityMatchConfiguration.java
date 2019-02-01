package com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class PrepareBulkEntityMatchConfiguration extends BaseStepConfiguration {

    @JsonProperty("customer_spaces_to_bump_up_version")
    private Map<EntityMatchEnvironment, List<String>> customerSpacesToBumpUpVersion;

    @JsonProperty("src_bucket")
    private String srcBucket;

    @JsonProperty("src_test_files")
    private List<String> srcTestFilePath;

    @JsonProperty("dest_test_dir")
    private String destTestDirectory;

    public Map<EntityMatchEnvironment, List<String>> getCustomerSpacesToBumpUpVersion() {
        return customerSpacesToBumpUpVersion;
    }

    public void setCustomerSpacesToBumpUpVersion(
            Map<EntityMatchEnvironment, List<String>> customerSpacesToBumpUpVersion) {
        this.customerSpacesToBumpUpVersion = customerSpacesToBumpUpVersion;
    }

    public String getSrcBucket() {
        return srcBucket;
    }

    public void setSrcBucket(String srcBucket) {
        this.srcBucket = srcBucket;
    }

    public List<String> getSrcTestFilePath() {
        return srcTestFilePath;
    }

    public void setSrcTestFilePath(List<String> srcTestFilePath) {
        this.srcTestFilePath = srcTestFilePath;
    }

    public String getDestTestDirectory() {
        return destTestDirectory;
    }

    public void setDestTestDirectory(String destTestDirectory) {
        this.destTestDirectory = destTestDirectory;
    }
}
