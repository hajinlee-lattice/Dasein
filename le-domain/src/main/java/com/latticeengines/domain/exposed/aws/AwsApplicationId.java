package com.latticeengines.domain.exposed.aws;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.yarn.api.records.ApplicationId;

public class AwsApplicationId extends ApplicationId {

    private String jobId;

    @Override
    protected void build() {
    }

    @Override
    public long getClusterTimestamp() {
        return 0;
    }

    @Override
    public int getId() {
        return 0;
    }

    @Override
    protected void setClusterTimestamp(long arg0) {
    }

    @Override
    protected void setId(int arg0) {
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = "application_" + jobId + "_aws";
    }

    public static boolean isAwsBatchJob(String jobId) {
        return jobId.matches("^application_.*_aws$");
    }

    public static String getAwsBatchJob(String jobId) {
        Matcher matcher = Pattern.compile("^application_(?<awsJobId>.*)_aws").matcher(jobId);
        if (matcher.matches()) {
            return matcher.group("awsJobId");
        }
        return jobId;

    }

    @Override
    public String toString() {
        return jobId;
    }

    @Override
    public int hashCode() {
        if (jobId == null)
            return 0;
        return new HashCodeBuilder().append(jobId).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!obj.getClass().equals(this.getClass())) {
            return false;
        }
        AwsApplicationId theOther = (AwsApplicationId) obj;
        return new EqualsBuilder().append(this.jobId, theOther.jobId).isEquals();
    }

}
