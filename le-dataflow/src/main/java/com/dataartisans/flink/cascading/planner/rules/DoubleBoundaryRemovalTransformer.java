/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink.cascading.planner.rules;

import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.ScopeExpression;
import cascading.flow.planner.iso.expression.TypeExpression;
import cascading.flow.planner.rule.PlanPhase;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.transformer.RuleReplaceTransformer;
import cascading.pipe.Boundary;

/**
 * Replaces double Boundarys by a single one
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class DoubleBoundaryRemovalTransformer extends RuleReplaceTransformer {

	public DoubleBoundaryRemovalTransformer() {
		super(PlanPhase.PostResolveAssembly, new DoubleBoundaryMatcher());
	}

	public static class DoubleBoundaryMatcher extends RuleExpression {

		public DoubleBoundaryMatcher() {
			super(
					(new ExpressionGraph()).
							arc(
									new TypeExpression(ElementCapture.Primary, Boundary.class, TypeExpression.Topo.LinearOut),
									ScopeExpression.ALL,
									new TypeExpression(ElementCapture.Secondary, Boundary.class, TypeExpression.Topo.LinearIn)
							)
			);
		}
	}

}
