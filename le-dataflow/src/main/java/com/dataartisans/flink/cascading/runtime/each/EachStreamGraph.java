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

package com.dataartisans.flink.cascading.runtime.each;

import cascading.flow.FlowNode;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.Gate;
import cascading.flow.stream.graph.IORole;
import cascading.flow.stream.graph.NodeStreamGraph;
import cascading.pipe.Boundary;
import cascading.pipe.CoGroup;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.tuple.Tuple;
import com.dataartisans.flink.cascading.runtime.boundaryStages.BoundaryInStage;
import com.dataartisans.flink.cascading.runtime.util.CollectorOutput;
import com.dataartisans.flink.cascading.runtime.util.FlinkFlowProcess;
import com.dataartisans.flink.cascading.runtime.boundaryStages.BoundaryOutStage;
import org.apache.flink.util.Collector;

@SuppressWarnings({"rawtypes"})
public class EachStreamGraph extends NodeStreamGraph {

	private BoundaryInStage sourceStage;
	private CollectorOutput sinkStage;

	public EachStreamGraph(FlinkFlowProcess flowProcess, FlowNode node, Boundary source) {

		super(flowProcess, node);

		sourceStage = handleHead(source);

		setTraps();
		setScopes();

		printGraph( node.getID(), "map", flowProcess.getCurrentSliceNum() );
		bind();
	}

	public void setTupleCollector(Collector<Tuple> tupleCollector) {
		this.sinkStage.setTupleCollector(tupleCollector);
	}

	public BoundaryInStage getSourceStage() {
		return this.sourceStage;
	}

	private BoundaryInStage handleHead( Boundary boundary) {

		BoundaryInStage sourceStage = (BoundaryInStage)createBoundaryStage(boundary, IORole.source);
		addHead( sourceStage );
		handleDuct( boundary, sourceStage );
		return sourceStage;
	}

	@Override
	protected Duct createBoundaryStage( Boundary boundary, IORole role ) {

		if(role == IORole.source) {
			this.sourceStage = new BoundaryInStage(this.flowProcess, boundary );
			return this.sourceStage;
		}
		else if(role == IORole.sink) {
			this.sinkStage = new BoundaryOutStage(this.flowProcess, boundary);
			return (BoundaryOutStage)this.sinkStage;
		}

		throw new IllegalArgumentException("Boundary must be either source or sink!");
	}

	@Override
	protected Gate createCoGroupGate(CoGroup coGroup, IORole ioRole) {
		throw new UnsupportedOperationException("Cannot create a CoGroup gate in a MapStreamGraph.");
	}

	@Override
	protected Gate createGroupByGate(GroupBy groupBy, IORole ioRole) {
		throw new UnsupportedOperationException("Cannot create a GroupBy gate in a MapStreamGraph.");
	}

	protected Gate createHashJoinGate( HashJoin join ) {
		throw new UnsupportedOperationException("Cannot create a HashJoin gate in a MapStreamGraph.");
	}

}
