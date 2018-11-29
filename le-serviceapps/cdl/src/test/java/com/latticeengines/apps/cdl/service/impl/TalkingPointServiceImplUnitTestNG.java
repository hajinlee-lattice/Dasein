package com.latticeengines.apps.cdl.service.impl;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.TalkingPointEntityMgr;
import com.latticeengines.domain.exposed.cdl.TalkingPoint;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class TalkingPointServiceImplUnitTestNG {

    @Mock
    private TalkingPointEntityMgr talkingPointEntityMgr;

    @InjectMocks
    private TalkingPointServiceImpl talkingPointService;

    @BeforeClass(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test(groups = "unit")
    public void testParseAttributeLook_One() {
        when(talkingPointEntityMgr.findAllByPlayName(anyString())).thenReturn(generateTalkingPointList_One());
        List<AttributeLookup> attributeLookups = talkingPointService.getAttributesInTalkingPointOfPlay("test");

        Assert.assertEquals(attributeLookups.size(), 1);
        Assert.assertEquals(attributeLookups.get(0).toString(), "Account.Name");
    }

    @Test(groups = "unit")
    public void testParseAttributeLook_Repeat() {
        when(talkingPointEntityMgr.findAllByPlayName(anyString())).thenReturn(generateTalkingPointList_Repeat());
        List<AttributeLookup> attributeLookups = talkingPointService.getAttributesInTalkingPointOfPlay("test");

        Assert.assertEquals(attributeLookups.size(), 1);
        Assert.assertEquals(attributeLookups.get(0).toString(), "Account.Name");
    }

    @Test(groups = "unit")
    public void testParseAttributeLook_Multiple() {
        when(talkingPointEntityMgr.findAllByPlayName(anyString())).thenReturn(generateTalkingPointList_Multiple());
        List<AttributeLookup> attributeLookups = talkingPointService.getAttributesInTalkingPointOfPlay("test");

        Assert.assertEquals(attributeLookups.size(), 3);
        Assert.assertTrue(attributeLookups.contains(new AttributeLookup(BusinessEntity.Account, "Name")));
        Assert.assertTrue(attributeLookups.contains(new AttributeLookup(BusinessEntity.Account, "ID")));
        Assert.assertTrue(attributeLookups.contains(new AttributeLookup(BusinessEntity.Contact, "Name")));
    }

    @Test(groups = "unit")
    public void testParseAttributeLook_CannotParse() {
        when(talkingPointEntityMgr.findAllByPlayName(anyString())).thenReturn(generateTalkingPointList_CannotParse());
        List<AttributeLookup> attributeLookups = talkingPointService.getAttributesInTalkingPointOfPlay("test");

        Assert.assertEquals(attributeLookups.size(), 0);
    }

    private List<TalkingPoint> generateTalkingPointList_One() {
        List<TalkingPoint> talkingPoints = new ArrayList<>();

        TalkingPoint tp1 = new TalkingPoint();
        tp1.setContent("{!Account.Name}");
        talkingPoints.add(tp1);

        return talkingPoints;
    }

    private List<TalkingPoint> generateTalkingPointList_Repeat() {
        List<TalkingPoint> talkingPoints = new ArrayList<>();

        TalkingPoint tp1 = new TalkingPoint();
        tp1.setContent("{!Account.Name}");
        talkingPoints.add(tp1);

        TalkingPoint tp2 = new TalkingPoint();
        tp2.setContent("{!Account.Name}");
        talkingPoints.add(tp2);

        return talkingPoints;
    }

    private List<TalkingPoint> generateTalkingPointList_Multiple() {
        List<TalkingPoint> talkingPoints = new ArrayList<>();
        TalkingPoint tp1 = new TalkingPoint();
        tp1.setContent("{!Contact.Name}");
        talkingPoints.add(tp1);

        TalkingPoint tp2 = new TalkingPoint();
        tp2.setContent("{!Account.Name}");
        talkingPoints.add(tp2);

        TalkingPoint tp3 = new TalkingPoint();
        tp3.setContent("{!Account.ID}");
        talkingPoints.add(tp3);

        return talkingPoints;
    }

    private List<TalkingPoint> generateTalkingPointList_CannotParse() {
        List<TalkingPoint> talkingPoints = new ArrayList<>();

        TalkingPoint tp1 = new TalkingPoint();
        tp1.setContent("{!Test}");
        talkingPoints.add(tp1);

        TalkingPoint tp2 = new TalkingPoint();
        tp2.setContent("{!@#$}");
        talkingPoints.add(tp2);

        return talkingPoints;
    }
}
