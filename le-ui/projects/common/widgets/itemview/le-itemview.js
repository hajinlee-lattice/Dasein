import "./le-itemview.scss";
import React, { Component } from "../../react-vendor";
import LePagination from "widgets/pagination/le-pagination";
import { store, injectAsyncReducer } from "store";

const actions = {
	set: payload => {
		return store.dispatch({
			type: "SET_LEITEMVIEW",
			payload: payload
		});
	}
};

const reducer = (state = {}, { type, payload }) => {
	switch (type) {
		case "SET_LEITEMVIEW":
			return {
				...state,
				...payload
			};
		default:
			return state;
	}
};

export default class LeItemView extends Component {
	constructor(props) {
		super(props);
	}

	componentDidMount() {
		injectAsyncReducer(store, this.getPath(), reducer);
		this.unsubscribe = store.subscribe(this.handleChange);
		actions.set(this.props.config);
	}

	componentWillUnmount() {
		this.unsubscribe();
	}

	handleChange = () => {
		const state = store.getState()[this.getPath()];
		this.setState(state);
	};

	getPath() {
		let { path = "view", name = "items" } = this.props;
		return `${path}.${name}`;
	}

	getContainerClass(view) {
		return "le-itemview " + (view.current == "grid" ? "tiles" : "list");
	}

	getView(state) {
		let name = state.stores.view;
		let view = store.getState()[name];
		let current = view.active || "grid";
		let component = state.tile.component;
		let controller = state.tile.controller;
		return { current, component, controller };
	}

	getStores(state) {
		let stores = state.stores;
		let ret = {};
		Object.keys(stores).forEach(key => {
			let split = stores[key].split(":");
			let path = split[0];
			let property = split[1];
			let conf = store.getState()[path] || {};

			console.log("<itemview> store", key, path, property, conf);
			if (typeof conf != undefined && property) {
				conf = conf[property];
			}

			ret[key] = conf;
		});
		return ret;
	}

	// this method applies sorting via the SortBy widget
	sortby(config, items) {
		let property = config.property;
		let direction = config.order;
		let ret = items.sort((a, b) => {
			switch (typeof a[property]) {
				case "string":
					return a[property].localeCompare(b[property]);
				case "number":
					return a[property] - b[property];
				default:
					throw new Error("Unsupported value to sort by");
			}
		});
		return direction == "-" ? ret.reverse() : ret;
	}

	// placeholder method for when I code the Refine/FilterBy widget
	refine(config, items) {
		return items;
	}

	// filter for the Query/Search widget
	query(config, items, self = {}) {
		// this represents the string to search for in items
		let query = config.query;

		self.state.query = query;

		// default properties to search if none provided in conf
		let properties = config.properties || [
			"displayName",
			"display_name",
			"DisplayName",
			"name",
			"Name",
			"label",
			"Label",
			"title"
		];

		// query checked in all prop values concatenated
		let ret = items.filter(item => {
			let value = "";

			properties.forEach(prop => {
				if (item[prop]) {
					value += item[prop];
				}
			});

			return !query || value.indexOf(query) >= 0;
		});

		return ret;
	}

	// applies pagination to items after all other sorting/filters
	pagination(items) {
		items = items.slice();
		let ret = [];
		let conf = this.state.pagination;
		ret = items.splice(conf.from || 0, conf.pagesize);
		return ret;
	}

	changePagination = page => {
		let pagination = this.state.pagination;
		pagination.from = page.from;
		pagination.to = page.to;
		this.state.pagination.current = page.current;
		actions.set(this.state);
	};

	getItems(data) {
		// <object path>:<items key> format
		let path = this.state.stores.items.split(":");

		// use <path>:items by default if no itemskey is provided
		let key = path.length > 1 ? path[1] : "items";
		return data.items[key] || [];
	}

	render() {
		let state = this.state;

		if (!state) {
			return "";
		}

		let view = this.getView(state) || {};
		let stores = this.getStores(state) || {};
		let items = stores.items || []; // this.getItems(stores) || [];

		// apply all sorting/filters in stores object
		Object.keys(stores).forEach(name => {
			if (typeof this[name] == "function") {
				items = this[name](stores[name], items, this);
			}
		});

		// apply the pagination filter
		let paginated_items = this.pagination(items);

		console.log("> render view", this);

		return (
			<div>
				<ul class={this.getContainerClass(view)}>
					{paginated_items.length > 0 && view.component
						? this.renderView(view, paginated_items, stores)
						: ""}
				</ul>
				<LePagination
					classesName="segment-pagination text-right"
					total={items.length}
					perPage={state.pagination.pagesize}
					start={state.pagination.current}
					callback={this.changePagination}
				/>
			</div>
		);
	}

	renderView(view, items, stores) {
		return items.map(item => {
			return React.createElement(
				view.component,
				this.getAttrs(view, item, this.state, stores)
			);
		});
	}

	getAttrs(view, item, store, stores) {
		let ret = Object.assign({});
		ret.controller = view.controller;
		ret.enrichments = store.enrichments;
		ret.cube = store.cube;
		ret.item = item;
		ret.view = view.current;
		ret.stores = stores;
		ret.config = this.state.tile.config;
		return ret;
	}
}
