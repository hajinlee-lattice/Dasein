package com.latticeengines.query.evaluator.restriction;

import com.latticeengines.domain.exposed.query.Restriction;
import com.querydsl.core.types.dsl.BooleanExpression;

public interface RestrictionResolver<T extends Restriction> {

    BooleanExpression resolve(T restriction);

}
