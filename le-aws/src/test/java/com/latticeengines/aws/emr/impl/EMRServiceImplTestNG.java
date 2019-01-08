package com.latticeengines.aws.emr.impl;

import java.util.List;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.latticeengines.aws.emr.EMRService;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-aws-context.xml" })
public class EMRServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private EMRService emrService;

    @Test(groups = "functional")
    public void testFindClusters() {
        Pattern pattern = Pattern.compile("^qa_[ab]_\\d+$");
        List<ClusterSummary> clusterSummaries = emrService.findClusters(clusterSummary -> {
            String name = clusterSummary.getName();
            return pattern.matcher(name).matches();
        });
        Assert.assertTrue(CollectionUtils.isNotEmpty(clusterSummaries));
    }


}
