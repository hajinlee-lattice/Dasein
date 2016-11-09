/**
 * @author rgonzalez
 *
 */
@FilterDefs({
@FilterDef(name = "tenantFilter", //
           parameters = @ParamDef(name = "tenantFilterId", type = "java.lang.Long")),
})
package com.latticeengines.domain.exposed.ulysses;

import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.FilterDefs;
import org.hibernate.annotations.ParamDef;

