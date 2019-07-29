import React, { Component } from "../../react-vendor";
import { store } from 'store';
import LePagination from "widgets/pagination/le-pagination";
import "./le-itemview.scss";

export default class LeItemView extends Component {
	constructor(props) {
		super(props);
	}

	componentDidMount() {
		this.unsubscribe = store.subscribe(this.handleChange);
	}

	componentWillUnmount() {
		this.unsubscribe();
	}

	handleChange = () => {
		const state = store.getState()[this.props.store];
		this.setState(state);
	}

	getContainerClass(view) {
		return "le-itemview " + (view.current == "grid" ? "tiles" : "list");
	}

	getView(state) {
		let name = state.itemview.stores.view;
		let current = state.itembar[name].config.active || "grid";
		let config = state.itemview.tile.views[current];
		let component = state.itemview.tile.component;
		return { current, config, component };
	}

	getStores(state) {
		let stores = state.itemview.stores;
		let ret = {};
		Object.keys(stores).forEach(store => {
			let name = stores[store];
			let conf = state.itembar[name] || {};
			ret[store] = conf.config ? conf.config : state[name];
		})
		return ret;
	}

	sortby(config, items) {
		let property = config.property;
		let direction = config.order;
		let ret = items.sort((a, b) => {
			switch (typeof a[property]) {
				case 'string':
					return a[property].localeCompare(b[property]);
				case 'number':
					return a[property] - b[property];
				default:
					throw new Error("Unsupported value to sort by");
			}
		});
		return direction == '-' ? ret.reverse() : ret;
	}

	refine(config, items) {
		return items;
	}

	query(config, items) {
		let query = config.query;
		let properties = config.properties || [
			'displayName', 'display_name', 'DisplayName', 'name', 'Name', 'title'
		];
		let ret = items.filter((item) => {
			let value = '';

			properties.forEach(prop => {
				if (item[prop]) {
					value += item[prop];
				}
			});

			return (!query || value.indexOf(query) >= 0);
		});

		return ret;
	}

	changePagination = (page) => {
		let pagination = this.state.itemview.pagination;
		pagination.from = page.from;
		pagination.to = page.to;
		this.state.itemview.pagination.current = page.current;
		this.setState(this.state);
	}

	pagination(items) {
		let ret = [];
		let conf = this.state.itemview.pagination;
		ret = items.splice(conf.from || 0, conf.pagesize);
		return ret;
	}

	getItems(data) {
		return data.items.slice();
	}

	render() {
		let state = this.state || {};
		if (!state.itemview) {
			return '';
		}

		let view = this.getView(state) || {};
		let stores = this.getStores(state) || {};
		let items = this.getItems(stores) || [];

		Object.keys(stores).forEach(name => {
			if (typeof this[name] == 'function') {
				items = this[name](stores[name], items);
			}
		});
		items = this.pagination(items);

		console.log('> render view', this);
		return (
			<div>
				<ul class={this.getContainerClass(view)}>
					{items.length > 0 && view.config
						? this.renderView(view, items)
						: ''
					}
				</ul>
				<LePagination
					classesName="segment-pagination text-right"
					total={stores.items.length}
					perPage={state.itemview.pagination.pagesize}
					start={state.itemview.pagination.current}
					callback={this.changePagination}
				/>
			</div>
		);
	}

	renderView(view, items) {
		return (
			items.map(item => {
				return React.createElement(view.component, this.getAttrs(
					view,
					item,
					this.state
				));
			})
		);
	}

	getAttrs(view, item, store) {
		let ret = Object.assign({});
		ret.context = store.context;
		ret.enrichments = store.enrichments;
		ret.cube = store.cube;
		ret.item = item;
		ret.view = view.current;
		return ret;
	}
}
