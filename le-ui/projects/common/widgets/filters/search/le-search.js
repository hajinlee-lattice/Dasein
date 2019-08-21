import "./le-search.scss";
import React, { Component } from "common/react-vendor";
import { store, injectAsyncReducer } from "store";

const actions = {
	set: payload => {
		return store.dispatch({
			type: "SET_LESEARCH",
			payload: payload
		});
	}
};

const reducer = (state = {}, { type, payload }) => {
	switch (type) {
		case "SET_LESEARCH":
			return {
				...state,
				...payload
			};
		default:
			return state;
	}
};

export default class LeSearch extends Component {
	constructor(props) {
		super(props);

		this.state = {
			placeholder: "Search",
			query: "",
			open: false
		};
	}

	componentDidMount() {
		let { path = "filters", name = "search" } = this.props;
		injectAsyncReducer(store, `${path}.${name}`, reducer);
		this.unsubscribe = store.subscribe(this.handleChange);
		actions.set(this.props.config);
	}

	componentWillUnmount() {
		this.unsubscribe();
	}

	handleChange = () => {
		let { path = "filters", name = "search" } = this.props;
		const state = store.getState()[`${path}.${name}`];
		this.setState(state);
	};

	clickToggle = () => {
		let state = Object.assign({}, this.state);
		state.open = !state.open;
		if (!state.open) {
			state.query = "";
		}
		actions.set(state);
	};

	changeQuery = event => {
		let state = Object.assign({}, this.state);
		state.query = event.target.value;
		actions.set(state);
	};

	getButtonClasses() {
		let classes = "fa " + (this.state.open ? "fa-times" : "fa-search");
		return classes;
	}

	render() {
		return (
			<div class="select-menu ng-search open-absolute">
				{this.state.open ? (
					<input
						type="text"
						className="form-control"
						placeholder={this.state.placeholder}
						onChange={this.changeQuery}
						value={this.state.query}
						autofocus
					/>
				) : (
					""
				)}
				<a
					className="button icon-button white-button"
					onClick={this.clickToggle}
				>
					<span className={this.getButtonClasses()} />
				</a>
			</div>
		);
	}
}
