package com.latticeengines.domain.exposed.datacloud.match;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.match.utils.MatchActorUtils;

public class MatchActorUtilsUnitTestNG {

    @Test(groups = "unit", dataProvider = "actorNamesToFull")
    public void testActorNamesToFull(String inputName, MatchActorType type, String expected) {
        Assert.assertEquals(MatchActorUtils.getFullActorName(inputName, type), expected);
    }

    @Test(groups = "unit", dataProvider = "actorNamesToShort")
    public void testActorNamesToShort(String inputName, MatchActorType type, String expected) {
        Assert.assertEquals(MatchActorUtils.getShortActorName(inputName, type), expected);
    }

    // InputName, ActorType, ExpectedName
    @DataProvider(name = "actorNamesToFull")
    public Object[][] prepareActorNamesToFull() {
        return new Object[][] { { "FuzzyMatch", MatchActorType.ANCHOR, "FuzzyMatchAnchorActor" }, //
                { "FuzzyMatchAnchorActor", MatchActorType.ANCHOR, "FuzzyMatchAnchorActor" }, //
                { "FuzzyMatch", MatchActorType.JUNCION, "FuzzyMatchJunctionActor" }, //
                { "FuzzyMatchJunctionActor", MatchActorType.JUNCION, "FuzzyMatchJunctionActor" }, //
                { "FuzzyMatch", MatchActorType.MICRO_ENGINE, "FuzzyMatchMicroEngineActor" }, //
                { "FuzzyMatchMicroEngineActor", MatchActorType.MICRO_ENGINE, "FuzzyMatchMicroEngineActor" }, //
        };
    }

    // InputName, ActorType, ExpectedName
    @DataProvider(name = "actorNamesToShort")
    public Object[][] prepareActorNamesToShort() {
        return new Object[][] {
                { "FuzzyMatch", MatchActorType.ANCHOR, "FuzzyMatch" },
                { "FuzzyMatchAnchorActor", MatchActorType.ANCHOR, "FuzzyMatch" },
                { "FuzzyMatch", MatchActorType.JUNCION, "FuzzyMatch" },
                { "FuzzyMatchJunctionActor", MatchActorType.JUNCION, "FuzzyMatch" },
                { "FuzzyMatch", MatchActorType.MICRO_ENGINE, "FuzzyMatch" },
                { "FuzzyMatchMicroEngineActor", MatchActorType.MICRO_ENGINE, "FuzzyMatch" },
        };
    }

}
