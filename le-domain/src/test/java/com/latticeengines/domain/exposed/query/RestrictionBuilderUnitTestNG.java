package com.latticeengines.domain.exposed.query;

import org.junit.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

import java.util.Arrays;

public class RestrictionBuilderUnitTestNG {

    @Test(groups = "unit")
    public void test() {
        Restriction aEq2 = Restriction.builder() //
                .let(BusinessEntity.Account, "A") //
                .eq(2) //
                .build();
        verify(aEq2);

        Restriction aGt2 = Restriction.builder().let(BusinessEntity.Account, "A")
                .gte(2)
                .build();
        verify(aGt2);

        Restriction aLte3 = Restriction.builder().let(BusinessEntity.Account, "A")
                .lte(3)
                .build();
        verify(aLte3);

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

        Restriction inCollection = Restriction.builder() //
                .let(BusinessEntity.Account, "A").inCollection(Arrays.asList(3, 5)).build();
        verify(inCollection);

        Restriction contactRestriction = Restriction.builder()
                .let(BusinessEntity.Contact, "A").eq(2).build();
        Restriction contactExists = Restriction.builder()
                .exists(BusinessEntity.Contact).that(contactRestriction).build();
        Restriction findAWithContact = Restriction.builder().and(aEq2, contactExists).build();
        verify(findAWithContact);
    }

    private void verify(Restriction restriction) {
        String serialized = JsonUtils.serialize(restriction);
        System.out.println(serialized);
        Assert.assertNotNull(serialized);
        Assert.assertNotNull(restriction);
    }

}
