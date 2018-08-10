package com.latticeengines.domain.exposed.graph;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

public class NameSpaceUtilUnitTestNG {
    private String ID = UUID.randomUUID().toString();
    private String A = UUID.randomUUID().toString() + ".user";
    private String B = UUID.randomUUID().toString() + ".3.63.076";
    private String C = UUID.randomUUID().toString();

    private NameSpaceUtil nameSpaceUtil = //
            new NameSpaceUtil(UUID.randomUUID().toString(), UUID.randomUUID().toString());

    @Test(groups = "unit")
    public void testForPostfixNS() {
        Map<String, String> nsMap = new HashMap<>();
        String ignoredKey = "RANDOME_KEY_" + UUID.randomUUID().toString();
        nsMap.put(ignoredKey, UUID.randomUUID().toString());
        nsMap.put(nameSpaceUtil.ENV_KEY, A);
        nsMap.put(nameSpaceUtil.VERSION_KEY, B);
        nsMap.put(nameSpaceUtil.TYPE_KEY, C);
        String vertexId = nameSpaceUtil.generateNSId(ID, nsMap, false);
        Assert.assertEquals(nameSpaceUtil.extractObjectIdFromGraphVertexId(vertexId, false), ID);

        Map<String, String> expectedNsMap = new HashMap<>(nsMap);
        expectedNsMap.remove(ignoredKey);
        System.out.println(vertexId);
        verifyMap(nameSpaceUtil.extractNsMapFromGraphVertexId(vertexId, false), expectedNsMap);
    }

    @Test(groups = "unit")
    public void testForPrefixNS() {
        Map<String, String> nsMap = new HashMap<>();
        String ignoredKey = "RANDOME_KEY_" + UUID.randomUUID().toString();
        nsMap.put(ignoredKey, UUID.randomUUID().toString());
        nsMap.put(nameSpaceUtil.ENV_KEY, A);
        nsMap.put(nameSpaceUtil.VERSION_KEY, B);
        nsMap.put(nameSpaceUtil.TYPE_KEY, C);
        Map<String, String> expectedNsMap = new HashMap<>(nsMap);
        expectedNsMap.remove(ignoredKey);

        String vertexId = nameSpaceUtil.generateNSId(ID, nsMap, true);
        Assert.assertEquals(nameSpaceUtil.extractObjectIdFromGraphVertexId(vertexId, true), ID);
        verifyMap(nameSpaceUtil.extractNsMapFromGraphVertexId(vertexId, true), expectedNsMap);
    }

    private void verifyMap(Map<String, String> extractNsMapFromGraphVertexId, Map<String, String> expectedNsMap) {
        Assert.assertEquals(extractNsMapFromGraphVertexId.size(), expectedNsMap.size());
        extractNsMapFromGraphVertexId.keySet().stream().forEach(k -> {
            Assert.assertTrue(expectedNsMap.containsKey(k));
            Assert.assertEquals(extractNsMapFromGraphVertexId.get(k), expectedNsMap.get(k));
        });
    }
}
