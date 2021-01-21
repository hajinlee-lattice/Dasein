package com.latticeengines.apps.core.util;

import static com.latticeengines.apps.core.util.S3ImportMessageUtils.KeyPart;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.jms.S3ImportMessageType;

public class S3ImportMessageUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testMessageType() {
        String dcpKey = "dropfolder/08vdzz2k/Projects/Project_xc51mzph/Source/Source_5aggfg55/drop/Account.csv";
        String dcpKey2 = "dropfolder/08vdzz2k/Projects/Project_xc51mzph/Sources/Source_5aggfg55/drop/Account.csv";
        String atlasKey = "dropfolder/08vdzz2k/Templates/DefaultSystem_AccountData/Account part1.CSV";
        String atlasKey2 = "dropfolder/08vdzz2k/Templates/DefaultSystem_AccountData/Account part1.CSV.gz";
        String atlasKey3 = "dropfolder/08vdzz2k/Templates/DefaultSystem_AccountData/Account part1.CSV.tar";
        String atlasKey4 = "dropfolder/08vdzz2k/Templates/DefaultSystem_AccountData/Account part1.CSV.tar.gz";
        String atlasKey5 = "dropfolder/08vdzz2k/Templates/DefaultSystem_AccountData/Account part1.CSV.zip";
        String invalidKey1 = "dropfolder/08vdzz2k/Projects/Project_xc51mzph/Source/Source_5aggfg55/Uploads/Account.csv";
        String invalidKey2 = "dropfolder/08vdzz2k/Projects/Project_xc51mzph/Source/Source_5aggfg55/drop/";
        String listSegmentKey = "datavision_segment/Lin_PA_Test/Segment_2020_11_23_06_37_39_UTC/Segment.zip";
        String inboundConnectionKey = "enterprise_integration/LETest1610446791713/143d058a-46fb-4fe4-bb04-123cf148de14/Account/Account_3aef6b8f-2478-44c3-be3a-7c53949a2372.csv";
        Assert.assertEquals(S3ImportMessageUtils.getMessageTypeFromKey(dcpKey), S3ImportMessageType.DCP);
        Assert.assertEquals(S3ImportMessageUtils.getMessageTypeFromKey(dcpKey2), S3ImportMessageType.DCP);
        Assert.assertEquals(S3ImportMessageUtils.getMessageTypeFromKey(atlasKey), S3ImportMessageType.Atlas);
        Assert.assertEquals(S3ImportMessageUtils.getMessageTypeFromKey(atlasKey2), S3ImportMessageType.Atlas);
        Assert.assertEquals(S3ImportMessageUtils.getMessageTypeFromKey(atlasKey3), S3ImportMessageType.Atlas);
        Assert.assertEquals(S3ImportMessageUtils.getMessageTypeFromKey(atlasKey4), S3ImportMessageType.Atlas);
        Assert.assertEquals(S3ImportMessageUtils.getMessageTypeFromKey(atlasKey5), S3ImportMessageType.Atlas);
        Assert.assertEquals(S3ImportMessageUtils.getMessageTypeFromKey(invalidKey1), S3ImportMessageType.UNDEFINED);
        Assert.assertEquals(S3ImportMessageUtils.getMessageTypeFromKey(invalidKey2), S3ImportMessageType.UNDEFINED);
        Assert.assertEquals(S3ImportMessageUtils.getMessageTypeFromKey(listSegmentKey), S3ImportMessageType.LISTSEGMENT);

        Assert.assertEquals(S3ImportMessageUtils.getKeyPart(dcpKey, S3ImportMessageType.DCP,
                S3ImportMessageUtils.KeyPart.PROJECT_ID), "Project_xc51mzph");
        Assert.assertEquals(S3ImportMessageUtils.getKeyPart(dcpKey, S3ImportMessageType.DCP,
                S3ImportMessageUtils.KeyPart.SOURCE_ID), "Source_5aggfg55");
        Assert.assertEquals(S3ImportMessageUtils.getKeyPart(dcpKey, S3ImportMessageType.DCP,
                S3ImportMessageUtils.KeyPart.FILE_NAME), "Account.csv");
        List<S3ImportMessageUtils.KeyPart> keyParts = new ArrayList<>();
        keyParts.add(KeyPart.TENANT_ID);
        keyParts.add(KeyPart.SOURCE_ID);
        keyParts.add(KeyPart.FILE_NAME);
        List<String> result = S3ImportMessageUtils.getKeyPartValues(inboundConnectionKey, S3ImportMessageType.INBOUND_CONNECTION, keyParts);
        Assert.assertEquals(result.size(), 3);
        Assert.assertEquals(result.get(0), "LETest1610446791713");
        Assert.assertEquals(result.get(1), "143d058a-46fb-4fe4-bb04-123cf148de14");
        Assert.assertEquals(result.get(2), "Account_3aef6b8f-2478-44c3-be3a-7c53949a2372.csv");
    }
}
