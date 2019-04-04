import React, { Component } from "common/react-vendor";
import "./le-search.scss";

export default class LeSearch extends Component {
	constructor(props) {
		super(props);

		this.state = {
			placeholder: 'Search',
			query: '',
			open: false
		};
	}

	componentDidMount() {
		this.setState(this.props.config);
	}

	clickToggle = () => {
		let state = Object.assign({}, this.state);
		state.open = !state.open;
		if (!state.open) {
			state.query = '';
		}
		this.setState(state);
		this.updateParent();
	}

	changeQuery = (event) => {
		this.state.query = event.target.value;
		this.setState(this.state);
		this.updateParent();
	}

	updateParent() {
		if (this.props.update) {
			this.props.update('search', this.state)
		}
	}

	getButtonClasses() {
		let classes = "fa " + (this.state.open ? "fa-times" : "fa-search");
		return classes;
	}

	render() {
		return (
			<div class="select-menu ng-search open-absolute">
				{
					this.state.open
						? <input
							type="text"
							className="form-control"
							placeholder={this.state.placeholder}
							onChange={this.changeQuery}
							value={this.state.query}
							autofocus
						/>
						: ''
				}
				<a className="button icon-button white-button"
					onClick={this.clickToggle}
				>
					<span className={this.getButtonClasses()}></span>
				</a>
			</div>
		);
	}
}
