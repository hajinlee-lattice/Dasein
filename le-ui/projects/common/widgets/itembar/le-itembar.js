import React, { Component } from "common/react-vendor";
import { store } from 'store';
import "./le-itembar.scss";

export default class LeItemBar extends Component {
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

	update = (path, payload) => {
		let state = Object.assign({}, this.state);
		let redux = state.context.ReduxStore;
		state.itembar[path].config = payload;
		redux.setItemView(state.itemview);
	}

	render() {
		let state = this.state || {};
		let config = state.itembar || {};
		let order = config.order || [];
		return (
			<ul class="le-itembar">
				{this.renderItems(order, config)}
			</ul>
		);
	}

	renderItems(items, config) {
		return items.map(name => {
			return (
				<li className={config[name].containerClass}>
					{this.renderItem(name, config[name])}
				</li>
			);
		});
	}

	renderItem(name, item) {
		return React.createElement(item.component, this.getAttrs(item, name));
	}

	getAttrs(attrs, name) {
		let ret = Object.assign({}, attrs);
		delete ret.component;
		delete ret.containerClass;
		ret.update = this.update;
		ret.name = name;
		return ret;
	}
}