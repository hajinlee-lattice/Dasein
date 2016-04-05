package com.latticeengines.dataplatform.service.impl.metadata;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.propdata.MatchClient;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;

public class MetadataProviderUnitTestNG {

    @Test(groups = "unit")
    public void testMatchClientUrl() {
        MatchClientDocument doc = new MatchClientDocument(MatchClient.PD128);

        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(doc.getHost())
                .port(doc.getPort())
                .db(doc.getDatabase())
                .user(doc.getUsername())
                .encryptedPassword(doc.getEncryptedPassword());
        DbCreds creds = new DbCreds(builder);

        MetadataProvider provider = new SQLServerMetadataProvider();
        Assert.assertEquals(provider.getConnectionString(creds),
                "jdbc:sqlserver://10.51.15.128:1433;databaseName=PropDataMatchDB;user=DLTransfer;password=free&NSE");
        Assert.assertEquals(
                provider.replaceUrlWithParamsAndTestConnection(doc.getUrl(), provider.getDriverClass(), creds),
                "jdbc:sqlserver://10.51.15.128:1433;databaseName=PropDataMatchDB;user=DLTransfer;password=free&NSE");
    }

}
