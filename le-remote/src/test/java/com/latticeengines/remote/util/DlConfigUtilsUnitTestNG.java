package com.latticeengines.remote.util;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DlConfigUtilsUnitTestNG {

    private static String sfdcConfig;
    private static String eloquaConfig;
    private static String marketoConfig;

    @BeforeClass(groups = {"unit", "functional"})
    public void setup() throws IOException {
        sfdcConfig = IOUtils.toString(
                Thread.currentThread().getContextClassLoader().getResourceAsStream("sfdc.config"));
        eloquaConfig = IOUtils.toString(
                Thread.currentThread().getContextClassLoader().getResourceAsStream("eloqua.config"));
        marketoConfig = IOUtils.toString(
                Thread.currentThread().getContextClassLoader().getResourceAsStream("mkto.config"));
    }

    @Test(groups = {"unit", "functional"}, dataProvider = "sfdcConfigProvider")
    public void parseSfdcUsername(String config, String username) {
        Assert.assertEquals(DlConfigUtils.parseSfdcUser(config), username);
    }

    @Test(groups = {"unit", "functional"}, dataProvider = "eloquaConfigProvider")
    public void parseEloquaUsernameCompany(String config, String username, String company) {
        Assert.assertEquals(DlConfigUtils.parseEloquaUsername(config), username);
        Assert.assertEquals(DlConfigUtils.parseEloquaCompany(config), company);
    }

    @Test(groups = {"unit", "functional"}, dataProvider = "marketoConfigProvider")
    public void parseMarketoUserIdURL(String config, String userId, String url) {
        Assert.assertEquals(DlConfigUtils.parseMarketoUserId(config), userId);
        Assert.assertEquals(DlConfigUtils.parseMarketoUrl(config), url);
    }

    @DataProvider(name = "sfdcConfigProvider")
    public static Object[][] getSfdcConfigProvider() {
        return new Object[][] {
                { wrapTag("<dataProvider name=\"SFDC_DataProvider\" autoMatch=\"False\" connectionString=\"URL=https://login.salesforce.com/services/Soap/u/33.0;User=apeters-widgettech@lattice-engines.com;Password=;SecurityToken=;Version=27.0;Timeout=100;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;BatchSize=2000;\" dbType=\"1002\" usedFor=\"31\" e=\"False\" />"),
                        "apeters-widgettech@lattice-engines.com" },
                { wrapTag("<dataProvider name=\"SFDC_DataProvider\" autoMatch=\"False\" connectionString=\"URL=https://login.salesforce.com/services/Soap/u/33.0;User=;Password=;SecurityToken=;Version=27.0;Timeout=100;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;BatchSize=2000;\" dbType=\"1002\" usedFor=\"31\" e=\"False\" />"),
                        "" },
                { wrapTag(""), "" },
                { sfdcConfig, "apeters-widgettech@lattice-engines.com" },
        };
    }

    @DataProvider(name = "eloquaConfigProvider")
    public static Object[][] getEloquaConfigProvider() {
        return new Object[][] {
                { wrapTag("<dataProvider name=\"Eloqua_DataProvider\" autoMatch=\"False\" connectionString=\"URL=https://login.eloqua.com/id;Company=TechnologyPartnerLatticeEngines;Username=Matt.Sable;Password=;APIType=SOAP;EntityType=Base;Timeout=300;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;BatchSize=200;MaxSizeOfErrorBatch=25;\" dbType=\"1006\" usedFor=\"31\" e=\"False\" />"),
                        "Matt.Sable", "TechnologyPartnerLatticeEngines" },
                { wrapTag("<dataProvider name=\"Eloqua_DataProvider\" autoMatch=\"False\" connectionString=\"URL=https://login.eloqua.com/id;Company=TechnologyPartnerLatticeEngines;Username=;Password=;APIType=SOAP;EntityType=Base;Timeout=300;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;BatchSize=200;MaxSizeOfErrorBatch=25;\" dbType=\"1006\" usedFor=\"31\" e=\"False\" />"),
                        "", "TechnologyPartnerLatticeEngines" },
                { wrapTag("<dataProvider name=\"Eloqua_DataProvider\" autoMatch=\"False\" connectionString=\"URL=https://login.eloqua.com/id;Company=;Username=Matt.Sable;Password=;APIType=SOAP;EntityType=Base;Timeout=300;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;BatchSize=200;MaxSizeOfErrorBatch=25;\" dbType=\"1006\" usedFor=\"31\" e=\"False\" />"),
                        "Matt.Sable", "" },
                { wrapTag(""), "", "" },
                { eloquaConfig, "Matt.Sable", "TechnologyPartnerLatticeEngines" },
        };
    }

    @DataProvider(name = "marketoConfigProvider")
    public static Object[][] getMarketoConfigProvider() {
        return new Object[][] {
                { wrapTag("<dataProvider name=\"Marketo_DataProvider\" autoMatch=\"False\" connectionString=\"URL=https://na-sj02.marketo.com/soap/mktows/2_0;UserID=latticeenginessandbox1_9026948050BD016F376AE6;EncryptionKey=;Timeout=10000;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;BatchSize=500;MaxSizeOfErrorBatch=25;\" dbType=\"1004\" usedFor=\"31\" e=\"False\" />"),
                        "latticeenginessandbox1_9026948050BD016F376AE6", "https://na-sj02.marketo.com/soap/mktows/2_0" },
                { wrapTag("<dataProvider name=\"Marketo_DataProvider\" autoMatch=\"False\" connectionString=\"URL=https://na-sj02.marketo.com/soap/mktows/2_0;UserID=;EncryptionKey=;Timeout=10000;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;BatchSize=500;MaxSizeOfErrorBatch=25;\" dbType=\"1004\" usedFor=\"31\" e=\"False\" />"),
                        "", "https://na-sj02.marketo.com/soap/mktows/2_0" },
                { wrapTag("<dataProvider name=\"Marketo_DataProvider\" autoMatch=\"False\" connectionString=\"URL=;UserID=latticeenginessandbox1_9026948050BD016F376AE6;EncryptionKey=;Timeout=10000;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;BatchSize=500;MaxSizeOfErrorBatch=25;\" dbType=\"1004\" usedFor=\"31\" e=\"False\" />"),
                        "latticeenginessandbox1_9026948050BD016F376AE6", "" },
                { wrapTag(""), "" },
                { marketoConfig, "latticeenginessandbox1_9026948050BD016F376AE6" },
        };
    }

    private static String wrapTag(String tag) {
        return "<dataProvider name=\"PlayMakerDB\" autoMatch=\"False\" connectionString=\"ServerName=bodcdevvcus88.dev.lattice.local\\SQL2012STD;Authentication=SQL Server Authentication;User=DataLoader;Password=;Database=;Schema=dbo;DateTimeOffsetOption=UtcDateTime;Timeout=100;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;BatchSize=2000;\" dbType=\"2\" usedFor=\"31\" e=\"False\" />"
                + tag + "<dataProvider name=\"SQL_DanteDB_DataProvider\" autoMatch=\"False\" connectionString=\"ServerName=;Database=;User=;Password=;Authentication=SQL Server Authentication;Schema=dbo;BatchSize=2000;\" dbType=\"2\" usedFor=\"1\" e=\"False\" />";
    }

}

//
