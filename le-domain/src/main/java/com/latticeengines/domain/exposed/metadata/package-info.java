/**
 * @author rgonzalez
 *
 */
@FilterDefs({
@FilterDef(name = "tenantFilter", //
           parameters = @ParamDef(name = "tenantFilterId", type = "java.lang.Long")),
@FilterDef(name = "typeFilter", //
           parameters = @ParamDef(name = "typeFilterId", type = "java.lang.Integer"))
})
package com.latticeengines.domain.exposed.metadata;

import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.FilterDefs;
import org.hibernate.annotations.ParamDef;

