package com.latticeengines.domain.exposed.graph;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

public class LabelUtilUnitTestNG {
    private String A = UUID.randomUUID().toString();
    private String B = UUID.randomUUID().toString().substring(0, 1);
    private String C = UUID.randomUUID().toString();

    @Test(groups = "unit", expectedExceptions = RuntimeException.class)
    public void testConcatenateLabels1() {
        LabelUtil.concatenateLabels(null);
    }

    @Test(groups = "unit", expectedExceptions = RuntimeException.class)
    public void testConcatenateLabels2() {
        LabelUtil.concatenateLabels(Arrays.asList(A, B + GraphConstants.LABEL_SEP, C));
    }

    @Test(groups = "unit")
    public void testConcatenateLabels3() {
        StringBuilder sb = new StringBuilder();
        sb.append(A);
        sb.append(GraphConstants.LABEL_SEP);
        sb.append(B);
        sb.append(GraphConstants.LABEL_SEP);
        sb.append(C);
        Assert.assertEquals(LabelUtil.concatenateLabels(Arrays.asList(A, B, C)), sb.toString());
        Assert.assertEquals(LabelUtil.concatenateLabels(Arrays.asList(A)), A);
    }

    @Test(groups = "unit")
    public void testExtractLabels() {
        List<String> lblList = LabelUtil.extractLabels(A);
        checkResult(lblList, A);
        StringBuilder sb = new StringBuilder();
        sb.append(A);
        sb.append(GraphConstants.LABEL_SEP);
        sb.append(B);
        sb.append(GraphConstants.LABEL_SEP);
        sb.append(C);
        lblList = LabelUtil.extractLabels(sb.toString());
        checkResult(lblList, B, A, C);
    }

    private void checkResult(List<String> lblList, String... expectedLabels) {
        Assert.assertEquals(lblList.size(), expectedLabels.length);
        Set<String> lblSet = new HashSet<>(lblList);
        Assert.assertEquals(lblSet.size(), expectedLabels.length);

        Arrays.asList(expectedLabels).stream().forEach(l -> Assert.assertTrue(lblSet.contains(l)));
    }
}
