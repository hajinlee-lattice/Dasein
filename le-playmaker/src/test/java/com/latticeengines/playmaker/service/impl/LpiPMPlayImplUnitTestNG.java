package com.latticeengines.playmaker.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class LpiPMPlayImplUnitTestNG {

    private LpiPMPlayImpl lpiPMPlayImpl;

    @Mock
    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    private String playId;
    private List<Integer> idList;

    @BeforeClass(groups = "unit")
    public void setup() {
        String randId = UUID.randomUUID().toString();
        playId = "play__" + randId;
        idList = new ArrayList<>();
        idList.add(1);

        MockitoAnnotations.initMocks(this);

        mockInternalResourceRestApiProxy();
        MultiTenantContext.setTenant(new Tenant("a.a.Production"));

        lpiPMPlayImpl = new LpiPMPlayImpl();

        lpiPMPlayImpl.setInternalResourceRestApiProxy(internalResourceRestApiProxy);
    }

    @Test(groups = "unit")
    public void testGetPlayCount() {
        lpiPMPlayImpl.getPlayCount(0, idList);
    }

    @Test(groups = "unit")
    public void testGetPlays() {
        lpiPMPlayImpl.getPlays(0L, 0, 2, idList);
    }

    private void mockInternalResourceRestApiProxy() {
        List<Play> plays = new ArrayList<>();
        Play play = new Play();
        play.setPid(1L);
        play.setName(playId);
        play.setDisplayName("My Play");
        play.setDescription("Play for business usecase");
        play.setUpdated(new Date());
        plays.add(play);
        when(internalResourceRestApiProxy //
                .getPlays(any(CustomerSpace.class))) //
                        .thenReturn(plays);
    }
}
