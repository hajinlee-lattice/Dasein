import React, { Component } from "common/react-vendor";
import "./le-buttons.scss";
import { store, injectAsyncReducer } from "store";

const actions = {
	set: payload => {
		return store.dispatch({
			type: "SET_LEBUTTONS",
			payload: payload
		});
	}
};

const reducer = (state = {}, { type, payload }) => {
	switch (type) {
		case "SET_LEBUTTONS":
			return {
				...state,
				...payload
			};
		default:
			return state;
	}
};

export default class LeButtons extends Component {
	constructor(props) {
		super(props);

		this.state = {
			active: "",
			items: []
		};

		console.log("<buttons> init", this);
	}

	componentDidMount() {
		let { path = "filters", name = "buttongroup" } = this.props;
		injectAsyncReducer(store, `${path}.${name}`, reducer);
		actions.set(this.props.config);
		this.setState(this.props.config);
		this.unsubscribe = store.subscribe(this.handleChange);
	}

	componentWillUnmount() {
		this.unsubscribe();
	}

	handleChange = () => {
		let { path = "filters", name = "buttongroup" } = this.props;
		const state = store.getState()[`${path}.${name}`];
		this.setState(state);
	};

	clickButton = item => {
		let state = Object.assign({}, this.state);
		state.active = item.name;
		//this.setState(state);
		actions.set(state);
	};

	render() {
		let config = this.state;
		let isDisabled = !config.activeColor;
		let activeColor = isDisabled ? "white" : config.activeColor || "green";

		return (
			<div class="le-buttons button-group">
				{config.items.map(item => {
					return (
						<button
							type="button"
							title={item.title}
							className={
								"button icon-button " +
								(config.active == item.name
									? activeColor
									: "white") +
								"-button"
							}
							onClick={() => {
								this.clickButton(item);
							}}
							disabled={isDisabled && config.active == item.name}
						>
							<span className={"fa " + item.icon} />
						</button>
					);
				})}
			</div>
		);
	}
}
