package com.latticeengines.domain.exposed.security;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class TicketUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        Ticket ticket = new Ticket("21f6b1ad-d4b3-4982-a395-6115cc993fbe.3D34A7DB6F65AC46");
        String deserializedStr = ticket.toString();
        Ticket deserializedTicket = JsonUtils.deserialize(deserializedStr, Ticket.class);
        
        assertEquals(deserializedTicket.getUniqueness(), "21f6b1ad-d4b3-4982-a395-6115cc993fbe");
        assertEquals(deserializedTicket.getRandomness(), "3D34A7DB6F65AC46");
    }
}
