import React, { Component } from "common/react-vendor";
import ReactRouter from "atlas/react/router";
import NgState from "atlas/ng-state";
import { actions, reducer } from "./segmentation.redux";
import { store, injectAsyncReducer } from "store";
import SegmentItem from "./segmentation.item";
import LeSort from "widgets/filters/sort/le-sort";
import LeSearch from "widgets/filters/search/le-search";
import LeButtons from "widgets/buttons/group/le-buttons";
import LeButton from "widgets/buttons/le-button";
import LeItemBar from "widgets/itembar/le-itembar";
import LeItemView from "widgets/itemview/le-itemview";

export default class SegmentationComponent extends Component {
	constructor(props) {
		super(props);
	}

	componentDidMount() {
		injectAsyncReducer(store, "segmentation", reducer);
		this.ReduxStore = actions;
		this.unsubscribe = store.subscribe(this.handleChange);

		this.DataCloudStore = ReactRouter.getRouter().ngservices[
			"DataCloudStore"
		];
		this.AttributesStore = ReactRouter.getRouter().ngservices[
			"AttributesStore"
		];

		let enrichments = this.DataCloudStore.enrichments || [];
		let cube = this.DataCloudStore.cube || {};

		actions.getEnrichments(enrichments);
		actions.getCube(cube);
		actions.getSegments();
	}

	componentWillUnmount() {
		this.unsubscribe();
	}

	handleChange = () => {
		const state = store.getState()["segmentation"];
		this.setState(state);
	};

	clickAddSegment = event => {
		console.log("clickAddSegment", event);
		let state = NgState.getAngularState();
		state.go("home.segment.explorer.attributes", { segment: "Create" });
	};

	getViewConfig = () => {
		return {
			stores: {
				items: "segmentation:segments",
				enrichments: "segmentation:enrichments",
				cube: "segmentation:cube",
				view: "segmentation.LeButtons",
				sortby: "segmentation.LeSort",
				query: "segmentation.LeSearch"
			},
			pagination: {
				current: 1,
				pagesize: 12
			},
			tile: {
				component: SegmentItem,
				controller: this,
				config: {
					clickTile: (event, segment) => {
						let state = NgState.getAngularState();

						console.log("clickTile", state, segment, event);

						event.preventDefault();
						event.stopPropagation();

						state.go(
							"home.segment.explorer.builder",
							{ segment: segment.name },
							{ reload: true }
						);
					},
					clickEdit: () => {},
					clickDelete: () => {},
					clickDuplicate: () => {}
				}
			}
		};
	};

	getSearchConfig = () => {
		return {
			query: "",
			open: true,
			placeholder: "Search Segments",
			properties: ["display_name", "description"]
		};
	};

	getSortConfig = () => {
		return {
			label: "Sort By",
			icon: "numeric",
			order: "-",
			property: "updated",
			visible: false,
			items: [
				{
					label: "Creation Date",
					icon: "numeric",
					property: "created"
				},
				{
					label: "Modified Date",
					icon: "numeric",
					property: "updated"
				},
				{ label: "Author Name", icon: "alpha", property: "created_by" },
				{
					label: "Segment Name",
					icon: "alpha",
					property: "display_name"
				}
			]
		};
	};

	getViewsConfig = () => {
		return {
			active: "grid",
			activeColor: "blue",
			items: [
				{ name: "grid", title: "Grid View", icon: "fa-th" },
				{ name: "list", title: "List View", icon: "fa-th-list" },
				{ name: "table", title: "Table View", icon: "fa-table" }
			]
		};
	};

	getAddSegmentConfig = () => {
		return {
			label: "Add Segment",
			classNames: ["blue-button"]
		};
	};

	render() {
		return (
			<section class="container segments-list">
				<LeItemBar store="segmentation">
					<LeSearch config={this.getSearchConfig()} />
					<LeSort config={this.getSortConfig()} />
					<LeButtons config={this.getViewsConfig()} />
					<LeButton
						config={this.getAddSegmentConfig()}
						containerClass="pull-right"
						callback={this.clickAddSegment}
					/>
				</LeItemBar>
				<LeItemView config={this.getViewConfig()} />
			</section>
		);
	}
}
