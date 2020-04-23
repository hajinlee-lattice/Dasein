package com.latticeengines.apps.core.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.jms.S3ImportMessageType;

public class S3ImportMessageUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testMessageType() {
        String dcpKey = "dropfolder/08vdzz2k/Projects/Project_xc51mzph/Source/Source_5aggfg55/drop/Account.csv";
        String dcpKey2 = "dropfolder/08vdzz2k/Projects/Project_xc51mzph/Sources/Source_5aggfg55/drop/Account.csv";
        String atlasKey = "dropfolder/08vdzz2k/Templates/DefaultSystem_AccountData/Account part1.CSV";
        String invalidKey1 = "dropfolder/08vdzz2k/Projects/Project_xc51mzph/Source/Source_5aggfg55/upload/Account.csv";
        String invalidKey2 = "dropfolder/08vdzz2k/Projects/Project_xc51mzph/Source/Source_5aggfg55/drop/";
        Assert.assertEquals(S3ImportMessageUtils.getMessageTypeFromKey(dcpKey), S3ImportMessageType.DCP);
        Assert.assertEquals(S3ImportMessageUtils.getMessageTypeFromKey(dcpKey2), S3ImportMessageType.DCP);
        Assert.assertEquals(S3ImportMessageUtils.getMessageTypeFromKey(atlasKey), S3ImportMessageType.Atlas);
        Assert.assertEquals(S3ImportMessageUtils.getMessageTypeFromKey(invalidKey1), S3ImportMessageType.UNDEFINED);
        Assert.assertEquals(S3ImportMessageUtils.getMessageTypeFromKey(invalidKey2), S3ImportMessageType.UNDEFINED);

        Assert.assertEquals(S3ImportMessageUtils.getKeyPart(dcpKey, S3ImportMessageType.DCP,
                S3ImportMessageUtils.KeyPart.PROJECT_ID), "Project_xc51mzph");
        Assert.assertEquals(S3ImportMessageUtils.getKeyPart(dcpKey, S3ImportMessageType.DCP,
                S3ImportMessageUtils.KeyPart.SOURCE_ID), "Source_5aggfg55");
        Assert.assertEquals(S3ImportMessageUtils.getKeyPart(dcpKey, S3ImportMessageType.DCP,
                S3ImportMessageUtils.KeyPart.FILE_NAME), "Account.csv");
    }
}
