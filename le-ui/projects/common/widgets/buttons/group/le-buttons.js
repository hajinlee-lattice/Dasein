import React, { Component } from "common/react-vendor";
import "./le-buttons.scss";

export default class LeButtons extends Component {
	constructor(props) {
		super(props);

		this.state = {
			active: '',
			items: []
		};

		console.log('<buttons> init', this);
	}

	componentDidMount() {
		this.setState(this.props.config);
	}

	clickButton = (item) => {
		let state = Object.assign({}, this.state);
		state.active = item.name;
		this.setState(state);
		this.updateParent(state);
	}

	updateParent(state) {
		if (this.props.update) {
			this.props.update(this.props.name, state)
		}
	}

	render() {
		let config = this.state;
		let isDisabled = !config.activeColor;
		let activeColor = isDisabled ? 'white' : config.activeColor || 'green';

		return (
			<div class="le-buttons button-group">
				{
					config.items.map(item => {
						return (
							<button
								type="button"
								title={item.title}
								className={"button icon-button " + (config.active == item.name ? activeColor : "white") + "-button"}
								onClick={() => { this.clickButton(item) }}
								disabled={isDisabled && config.active == item.name}
							>
								<span className={'fa ' + item.icon}></span>
							</button>
						);
					})
				}
			</div>
		);
	}
}
