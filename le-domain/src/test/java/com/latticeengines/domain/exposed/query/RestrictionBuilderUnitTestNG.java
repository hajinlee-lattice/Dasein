package com.latticeengines.domain.exposed.query;

import org.junit.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class RestrictionBuilderUnitTestNG {

    @Test(groups = "unit")
    public void test() {
        Restriction aEq2 = Restriction.builder() //
                .let(BusinessEntity.Account, "A") //
                .eq(2) //
                .build();
        verify(aEq2);

        Restriction bNeqHello = Restriction.builder() //
                .let(BusinessEntity.Account, "B") //
                .neq("hello") //
                .build();
        verify(bNeqHello);

        Restriction cBtwn0And10 = Restriction.builder() //
                .let(BusinessEntity.Account, "C") //
                .in(0, 10) //
                .build();
        verify(cBtwn0And10);

        Restriction dNotBtwn0And10 = Restriction.builder() //
                .let(BusinessEntity.Account, "D") //
                .not().in(0, 10) //
                .build();
        verify(dNotBtwn0And10);

        Restriction aAndB = Restriction.builder() //
                .and(aEq2, bNeqHello) //
                .build();
        verify(aAndB);

        Restriction cOrD = Restriction.builder() //
                .or(cBtwn0And10, dNotBtwn0And10) //
                .build();
        verify(cOrD);

        Restriction finalAnd = Restriction.builder() //
                .and(aAndB, cOrD)
                .build();
        verify(finalAnd);

    }

    private void verify(Restriction restriction) {
        String serialized = JsonUtils.serialize(restriction);
        System.out.println(serialized);
        Assert.assertNotNull(serialized);
        Assert.assertNotNull(restriction);
    }

}
