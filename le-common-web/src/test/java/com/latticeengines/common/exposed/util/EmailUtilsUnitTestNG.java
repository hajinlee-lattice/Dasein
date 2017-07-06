package com.latticeengines.common.exposed.util;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

public class EmailUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testParseEmails() throws Exception {
        String emailsString = "[\"1@1.com\", \"2@2.com\"]";
        List<String> emailList = EmailUtils.parseEmails(emailsString);
        Assert.assertEquals(emailList.size(), 2);
    }

}
