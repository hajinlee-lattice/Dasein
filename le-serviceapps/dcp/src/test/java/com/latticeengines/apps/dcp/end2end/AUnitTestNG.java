package com.latticeengines.apps.dcp.end2end;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.testng.annotations.Test;

public class AUnitTestNG {


    @Test
    public void testParseS3() {
        String usageReportPath = "s3://my-bucket/my/path";
        Pattern pattern = Pattern.compile("s3://(?<bucket>[^/]+)/(?<prefix>.*)");
        Matcher matcher = pattern.matcher(usageReportPath);
        System.out.println(matcher.matches());
        String s3Bucket = matcher.group("bucket");
        String s3Prefix = matcher.group("prefix");
        System.out.println(s3Bucket);
        System.out.println(s3Prefix);
    }

}
