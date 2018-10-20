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

package com.dataartisans.flink.cascading.runtime.sink;

import com.dataartisans.flink.cascading.runtime.util.FlinkFlowProcess;

import cascading.flow.FlowNode;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.Gate;
import cascading.flow.stream.graph.IORole;
import cascading.flow.stream.graph.NodeStreamGraph;
import cascading.pipe.Boundary;
import cascading.pipe.CoGroup;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;

@SuppressWarnings({ "rawtypes" })
public class SinkStreamGraph extends NodeStreamGraph {

    private SinkBoundaryInStage sourceStage;

    public SinkStreamGraph(FlinkFlowProcess flowProcess, FlowNode node, Boundary source) {

        super(flowProcess, node);

        sourceStage = handleHead(source);

        setTraps();
        setScopes();

        printGraph(node.getID(), "sink", flowProcess.getCurrentSliceNum());
        bind();
    }

    public SinkBoundaryInStage getSourceStage() {
        return this.sourceStage;
    }

    private SinkBoundaryInStage handleHead(Boundary boundary) {

        SinkBoundaryInStage sourceStage = (SinkBoundaryInStage) createBoundaryStage(boundary,
                IORole.source);
        addHead(sourceStage);
        handleDuct(boundary, sourceStage);
        return sourceStage;
    }

    @Override
    protected Duct createBoundaryStage(Boundary boundary, IORole role) {

        if (role == IORole.source) {
            this.sourceStage = new SinkBoundaryInStage(this.flowProcess, boundary, this.node);
            return this.sourceStage;
        } else {
            throw new UnsupportedOperationException(
                    "Only source boundary allowed in SinkStreamGraph");
        }
    }

    @Override
    protected Gate createCoGroupGate(CoGroup coGroup, IORole ioRole) {
        throw new UnsupportedOperationException(
                "Cannot create a CoGroup gate in a SinkStreamGraph");
    }

    @Override
    protected Gate createGroupByGate(GroupBy groupBy, IORole ioRole) {
        throw new UnsupportedOperationException(
                "Cannot create a GroupBy gate in a SinkStreamGraph");
    }

    protected Gate createHashJoinGate(HashJoin join) {
        throw new UnsupportedOperationException(
                "Cannot create a HashJoin gate in a SinkStreamGraph");
    }

}
