package com.latticeengines.domain.exposed.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.aws.AwsApplicationId;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.FakeApplicationId;

public final class ApplicationIdUtils {

    private static final String AWS_JOBID_PATTERN_STR = "[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}";
    private static final String YARN_JOBID_PATTERN_STR = "\\d{13}_\\d{4,}";
    private static final String APPLICATION_PREFIX = "application_";
    private static final String AWS_SUFFIX = "_aws";

    private static final Pattern AWS_JOBID_PATTERN = Pattern.compile(AWS_JOBID_PATTERN_STR);
    private static final Pattern YARN_JOBID_PATTERN = Pattern.compile(YARN_JOBID_PATTERN_STR);

    private static final Pattern AWS_APPID_PATTERN = //
            Pattern.compile(APPLICATION_PREFIX + "(?<jobId>" + AWS_JOBID_PATTERN_STR + ")" + AWS_SUFFIX);
    private static final Pattern YARN_APPID_PATTERN = //
            Pattern.compile(APPLICATION_PREFIX + "(?<jobId>" + YARN_JOBID_PATTERN_STR + ")");

    public static String expandToApplicationId(String jobId) {
        if (AWS_JOBID_PATTERN.matcher(jobId).matches()) {
            AwsApplicationId awsApplicationId = new AwsApplicationId();
            awsApplicationId.setJobId(jobId);
            return awsApplicationId.getApplicationId();
        } else if (YARN_JOBID_PATTERN.matcher(jobId).matches()) {
            return APPLICATION_PREFIX + jobId;
        } else {
            throw new IllegalArgumentException("Unknown jobId format: " + jobId);
        }
    }

    public static String stripJobId(String applicationId) {
        Matcher matcher = YARN_APPID_PATTERN.matcher(applicationId);
        if (matcher.matches()) {
            return matcher.group("jobId");
        }
        matcher = AWS_APPID_PATTERN.matcher(applicationId);
        if (matcher.matches()) {
            return matcher.group("jobId");
        }
        throw new IllegalArgumentException("Malformed ApplicationId: " + applicationId);
    }

    public static ApplicationId toApplicationIdObj(String appId) {
        if (StringUtils.isBlank(appId)) {
            return null;
        }
        if (FakeApplicationId.isFakeApplicationId(appId)) {
            Long workflowPid = FakeApplicationId.toWorkflowJobPid(appId);
            return new FakeApplicationId(workflowPid);
        }
        return ApplicationId.fromString(appId);
    }

    public static boolean isFakeApplicationId(String appId) {
        return FakeApplicationId.isFakeApplicationId(appId);
    }
}
