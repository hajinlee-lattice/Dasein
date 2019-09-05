package com.latticeengines.cdl.workflow.steps.play;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.cdl.workflow.steps.play.CampaignLaunchProcessor.ProcessedFieldMappingMetadata;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchContext.PlayLaunchContextBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

public class FrontEndQueryCreatorTestNG {

    private static final Logger log = LoggerFactory.getLogger(FrontEndQueryCreatorTestNG.class);

    private PlayLaunchContext playLaunchContext;

    private List<ColumnMetadata> fieldMappingMetadata;

    private CustomerSpace customerSpace;

    private PlayLaunch playLaunch;

    @Mock
    private BatonService batonService;

    @InjectMocks
    private FrontEndQueryCreator frontEndQueryCreator = new FrontEndQueryCreator();

    @SuppressWarnings("unchecked")
    @BeforeClass(groups = "functional")
    public void setup() throws IOException {
        URL url = ClassLoader.getSystemResource("com/latticeengines/cdl/workflow/campaignLaunchWorkflow/response.json");
        File file = new File(url.getFile());
        String str = FileUtils.readFileToString(file, Charset.defaultCharset());
        List<Object> rawObject = JsonUtils.deserialize(str, List.class);
        fieldMappingMetadata = JsonUtils.convertList(rawObject, ColumnMetadata.class);
        log.info("size of fieldMappingMetadata " + fieldMappingMetadata.size());
        playLaunch = new PlayLaunch();
        playLaunch.setDestinationAccountId("sfdc3");
        customerSpace = CustomerSpace.parse("tenant");
        playLaunchContext = new PlayLaunchContextBuilder().customerSpace(customerSpace) //
                .playLaunch(playLaunch) //
                .accountFrontEndQuery(new FrontEndQuery()) //
                .contactFrontEndQuery(new FrontEndQuery()) //
                .fieldMappingMetadata(fieldMappingMetadata) //
                .build();
        frontEndQueryCreator.init();
        MockitoAnnotations.initMocks(this);
        when(batonService.isEntityMatchEnabled(any(CustomerSpace.class))).thenReturn(true);
    }

    @Test(groups = "functional")
    public void testPrepareLookupsForFrontEndQueries() {
        ProcessedFieldMappingMetadata result = frontEndQueryCreator.prepareLookupsForFrontEndQueries(playLaunchContext,
                true);
        Assert.assertNotNull(result);
        Assert.assertEquals(17, result.getAccountColsRecIncluded().size());
        Assert.assertEquals(4, result.getAccountColsRecNotIncludedNonStd().size());
        Assert.assertEquals(7, result.getContactCols().size());
        Assert.assertEquals(56, result.getAccountColsRecNotIncludedStd().size());
    }
}
