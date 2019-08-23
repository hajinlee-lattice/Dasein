import React, { Component } from "common/react-vendor";
import { store } from "store";
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
	};

	render() {
		let state = this.state || {};
		let config = state.itembar || {};
		let items = config.order || this.props.children;

		return <ul class="le-itembar">{this.renderItems(items, config)}</ul>;
	}

	renderItems(items, config) {
		return items.map(element => {
			if (config.order) {
				return (
					<li className={config[element].containerClass}>
						{this.renderItem(element, config[element])}
					</li>
				);
			} else {
				let name = element.type.name;
				let props = this.getAttrs({}, name);
				let clone = React.cloneElement(element, props);

				return (
					<li className={element.props.containerClass}>{clone}</li>
				);
			}
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
		ret.path = this.props.store;
		ret.name = name;
		return ret;
	}
}
