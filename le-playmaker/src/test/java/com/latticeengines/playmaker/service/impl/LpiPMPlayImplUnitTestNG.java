package com.latticeengines.playmaker.service.impl;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.db.exposed.util.MultiTenantContext;

public class LpiPMPlayImplUnitTestNG {

    private LpiPMPlayImpl lpiPMPlayImpl;

    @Mock
    private PlayProxy playProxy;

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

        lpiPMPlayImpl.setPlayProxy(playProxy);
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
        when(playProxy.getPlays(anyString(), isNull(Boolean.class), isNull(String.class))).thenReturn(plays);
    }
}
