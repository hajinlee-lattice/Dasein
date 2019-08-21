import React, { Component } from "common/react-vendor";
import propTypes from "prop-types";
import "./le-sort.scss";
import { store, injectAsyncReducer } from "store";
import { connect } from "react-redux";

const CONST = {
	SET_LESORT_CONFIG: "SET_LESORT_CONFIG",
	SET_LESORT_TOGGLE: "SET_LESORT_TOGGLE",
	SET_LESORT_ORDER: "SET_LESORT_ORDER",
	SET_LESORT_PROPERTY: "SET_LESORT_PROPERTY"
};

const actions = {
	setConfig: payload => {
		return store.dispatch({
			type: "SET_LESORT_CONFIG",
			payload: payload
		});
	},
	setProperty: payload => {
		return store.dispatch({
			type: "SET_LESORT_PROPERTY",
			payload: payload
		});
	},
	setToggle: payload => {
		return store.dispatch({
			type: "SET_LESORT_TOGGLE"
		});
	},
	setOrder: payload => {
		return store.dispatch({
			type: "SET_LESORT_ORDER"
		});
	}
};

const reducer = (state = {}, { type, payload }) => {
	console.log("> reducer", type, payload);
	switch (type) {
		case CONST.SET_LESORT_CONFIG:
			return {
				...state,
				...payload
			};
		case CONST.SET_LESORT_PROPERTY:
			return {
				...state,
				label: payload.label,
				property: payload.property,
				icon: payload.icon
			};
		case CONST.SET_LESORT_TOGGLE:
			return {
				...state,
				visible: !state.visible
			};
		case CONST.SET_LESORT_ORDER:
			return {
				...state,
				order: state.order == "-" ? "" : "-"
			};
		default:
			return state;
	}
};

export default class LeSort extends Component {
	constructor(props) {
		super(props);

		this.state = {
			label: "",
			visible: false,
			items: [],
			farts: {
				poop: {
					sharts: false
				}
			}
		};

		//let bentest = this.state.farts.indigo.hi || "burp";

		console.log("sort init", this.state?.farts?.poop?.sharts ?? "burp");
	}

	componentDidMount() {
		let { path = "filters", name = "sort" } = this.props;
		injectAsyncReducer(store, `${path}.${name}`, reducer);
		this.unsubscribe = store.subscribe(this.handleChange);
		actions.setConfig(this.props.config);
	}

	componentWillUnmount() {
		this.unsubscribe();
	}

	handleChange = () => {
		let { path = "filters", name = "sort" } = this.props;
		const state = store.getState()[`${path}.${name}`];
		this.setState(state);
	};

	getButtonClasses(config, additional) {
		let classes = "button white-button icon-button select-";
		classes += additional ? additional : "";
		classes += config.visible ? " open" : "";
		return classes;
	}

	getIconClasses(item) {
		let classes = "fa fa-sort-" + (item.icon || "amount");
		classes += "-" + (this.state.order == "" ? "asc" : "desc");
		return classes;
	}

	render() {
		let config = this.state;

		return (
			<div class="select-menu white-select-menu sort-select">
				<button
					type="button"
					title="Sort Property"
					className={this.getButtonClasses(config, "label")}
					onClick={actions.setToggle}
				>
					{config.label}
				</button>
				<button
					type="button"
					title="Sort Direction"
					className={this.getButtonClasses(config, "more")}
					onClick={actions.setOrder}
				>
					<span className={this.getIconClasses(config)} />
				</button>
				{config.visible && config.items.length > 0 ? (
					<ul class="model-menu" onClick={this.clickToggle}>
						{this.renderItems(config.items)}
					</ul>
				) : (
					""
				)}
			</div>
		);
	}

	renderItems(items) {
		return items.map(item => {
			if (item.if == undefined || item.if) {
				return (
					<li
						className={item.class}
						onClick={() => {
							actions.setToggle();
							actions.setProperty(item);
						}}
					>
						<a href="javascript:void(0)">{item.label}</a>
						{this.renderItemIcon(item)}
					</li>
				);
			}
		});
	}

	renderItemIcon(item) {
		if (item.icon) {
			return <i className={this.getIconClasses(item)} />;
		}
	}
}

function mapStateToProps(state) {
	return {
		config: state.segmentation.LeSort
	};
}

//export default connect(mapStateToProps)(LeSort);

LeSort.propTypes = {
	config: propTypes.object.isRequired
};
