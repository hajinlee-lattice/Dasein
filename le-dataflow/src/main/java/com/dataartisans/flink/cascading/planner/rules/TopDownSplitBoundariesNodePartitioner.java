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

import static cascading.flow.planner.rule.PlanPhase.PartitionNodes;

import cascading.flow.planner.iso.ElementAnnotation;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.PathScopeExpression;
import cascading.flow.planner.iso.expression.TypeExpression;
import cascading.flow.planner.iso.finder.SearchOrder;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.elementexpression.BoundariesElementExpression;
import cascading.flow.planner.rule.expressiongraph.NoGroupJoinMergeBoundaryTapExpressionGraph;
import cascading.flow.planner.rule.partitioner.ExpressionRulePartitioner;
import cascading.flow.stream.graph.IORole;

public class TopDownSplitBoundariesNodePartitioner extends ExpressionRulePartitioner {
    public TopDownSplitBoundariesNodePartitioner() {
        super(PartitionNodes,

                new RuleExpression(new NoGroupJoinMergeBoundaryTapExpressionGraph(),
                        new TopDownSplitBoundariesExpressionGraph()),

                new ElementAnnotation(ElementCapture.Include, IORole.sink));
    }

    private static class TopDownSplitBoundariesExpressionGraph extends ExpressionGraph {
        public TopDownSplitBoundariesExpressionGraph() {
            super(SearchOrder.Topological);

            this.arc(new BoundariesElementExpression(ElementCapture.Primary,
                    TypeExpression.Topo.Split),

                    PathScopeExpression.ANY,

                    new BoundariesElementExpression());
        }
    }

}
