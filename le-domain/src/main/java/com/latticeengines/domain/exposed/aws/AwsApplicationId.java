package com.latticeengines.domain.exposed.aws;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.yarn.api.records.ApplicationId;

public class AwsApplicationId extends ApplicationId {

    private String jobId;
    private String applicationId;
    private static final String UUID_PATTERN = "[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}";

    public static AwsApplicationId fromJobId(String jobId) {
        AwsApplicationId awsApplicationId = new AwsApplicationId();
        awsApplicationId.setJobId(jobId);
        return awsApplicationId;
    }

    public static AwsApplicationId fromString(String appIdStr) {
        if (AwsApplicationId.isAwsBatchJob(appIdStr)) {
            String jobId = AwsApplicationId.getAwsBatchJobId(appIdStr);
            return AwsApplicationId.fromJobId(jobId);
        } else {
            throw new IllegalArgumentException("Malformed AwsApplicationId " + appIdStr);
        }
    }

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

    public String getApplicationId() {
        return applicationId;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
        this.applicationId = "application_" + jobId + "_aws";
    }

    public static boolean isAwsBatchJob(String applicationId) {
      return applicationId.toLowerCase().matches("^application_" + UUID_PATTERN + "_aws$");
    }

    public static String getAwsBatchJobId(String applicationId) {
        Matcher matcher = Pattern.compile("^application_(?<awsJobId>.*)_aws").matcher(applicationId);
        if (matcher.matches()) {
            return matcher.group("awsJobId");
        }
        return applicationId;

    }

    @Override
    public String toString() {
        return applicationId;
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
