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

import static cascading.flow.planner.rule.PlanPhase.BalanceAssembly;

import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.iso.finder.SearchOrder;
import cascading.flow.planner.iso.transformer.InsertionGraphTransformer;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.transformer.RuleInsertionTransformer;
import cascading.pipe.Merge;

/**
 * Injects a Boundary after a Merge in order to split of the Merge as a separate node.
 */
public class BoundaryAfterMergeTransformer extends RuleInsertionTransformer
{
	public BoundaryAfterMergeTransformer() {
		super(
				BalanceAssembly,
				new MergeMatcher(),
				BoundaryElementFactory.BOUNDARY_FACTORY,
				InsertionGraphTransformer.Insertion.After
		);
	}

	public static class MergeMatcher extends RuleExpression
	{
		public MergeMatcher()
		{
			super( new MergeGraph() );
		}
	}

	public static class MergeGraph extends ExpressionGraph {

		public MergeGraph() {

			super(SearchOrder.ReverseTopological, new FlowElementExpression(ElementCapture.Primary, Merge.class));

		}
	}

}
