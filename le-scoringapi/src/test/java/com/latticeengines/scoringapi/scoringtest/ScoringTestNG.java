package com.latticeengines.scoringapi.scoringtest;

import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Factory;

public class ScoringTestNG {
    @Factory
    public Object[] createScoringTestExecutors() throws Exception {
        List<ScoringTestExecutor> executors = new ArrayList<ScoringTestExecutor>();

        URL parentDirURL = getClass().getClassLoader().getResource("tests");

        TestDirectoryReader reader = new TestDirectoryReader(Paths.get(parentDirURL.toURI()).toString());
        List<TestDefinition> definitions = reader.read();
        for (TestDefinition definition : definitions) {
            executors.add(new ScoringTestExecutor(definition));
        }

        return executors.toArray();
    }

}
