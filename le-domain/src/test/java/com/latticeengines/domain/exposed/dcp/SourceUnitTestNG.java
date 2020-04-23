package com.latticeengines.domain.exposed.dcp;

import org.testng.Assert;
import org.testng.annotations.Test;

public class SourceUnitTestNG {

    @Test(groups = "unit")
    public void testGetRelativePath() {
        Source source = new Source();
        String relativePath  = "Projects/Project_f1iaf08m/Source/Source_07ckqmax/";
        source.setSourceFullPath(String.format("%s/%s/%s", "lattice-engines-dev", "dropfolder/abcdefgh", relativePath));
        source.setDropFullPath(source.getSourceFullPath() + "drop/");
        Assert.assertEquals(source.getRelativePathUnderDropfolder(), relativePath);
    }

}
