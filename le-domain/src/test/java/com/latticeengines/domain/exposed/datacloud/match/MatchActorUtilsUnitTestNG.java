package com.latticeengines.domain.exposed.datacloud.match;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.actors.ActorType;
import com.latticeengines.domain.exposed.datacloud.match.utils.MatchActorUtils;

public class MatchActorUtilsUnitTestNG {

    @Test(groups = "unit", dataProvider = "actorNamesToFull")
    public void testActorNamesToFull(String inputName, ActorType type, String expected) {
        Assert.assertEquals(MatchActorUtils.getFullActorName(inputName, type), expected);
    }

    @Test(groups = "unit", dataProvider = "actorNamesToShort")
    public void testActorNamesToShort(String inputName, ActorType type, String expected) {
        Assert.assertEquals(MatchActorUtils.getShortActorName(inputName, type), expected);
    }

    // InputName, ActorType, ExpectedName
    @DataProvider(name = "actorNamesToFull")
    public Object[][] prepareActorNamesToFull() {
        return new Object[][] { { "FuzzyMatch", ActorType.ANCHOR, "FuzzyMatchAnchorActor" }, //
                { "FuzzyMatchAnchorActor", ActorType.ANCHOR, "FuzzyMatchAnchorActor" }, //
                { "FuzzyMatch", ActorType.JUNCION, "FuzzyMatchJunctionActor" }, //
                { "FuzzyMatchJunctionActor", ActorType.JUNCION, "FuzzyMatchJunctionActor" }, //
                { "FuzzyMatch", ActorType.MICRO_ENGINE, "FuzzyMatchMicroEngineActor" }, //
                { "FuzzyMatchMicroEngineActor", ActorType.MICRO_ENGINE, "FuzzyMatchMicroEngineActor" }, //
        };
    }

    // InputName, ActorType, ExpectedName
    @DataProvider(name = "actorNamesToShort")
    public Object[][] prepareActorNamesToShort() {
        return new Object[][] {
                { "FuzzyMatch", ActorType.ANCHOR, "FuzzyMatch" },
                { "FuzzyMatchAnchorActor", ActorType.ANCHOR, "FuzzyMatch" },
                { "FuzzyMatch", ActorType.JUNCION, "FuzzyMatch" },
                { "FuzzyMatchJunctionActor", ActorType.JUNCION, "FuzzyMatch" },
                { "FuzzyMatch", ActorType.MICRO_ENGINE, "FuzzyMatch" },
                { "FuzzyMatchMicroEngineActor", ActorType.MICRO_ENGINE, "FuzzyMatch" },
        };
    }

}
