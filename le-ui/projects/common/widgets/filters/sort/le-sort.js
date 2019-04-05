import React, { Component } from "common/react-vendor";
import propTypes from "prop-types";
import "./le-sort.scss";

export default class LeSort extends Component {
	constructor(props) {
		super(props);

		this.state = {
			label: '',
			visible: false,
			items: []
		};
	}

	componentDidMount() {
		this.setState(this.props.config);
	}

	clickToggle = () => {
		this.state.visible = !this.state.visible;
		this.setState(this.state);
	}

	clickOrder = () => {
		this.state.order = this.state.order == '-' ? '' : '-';
		this.setState();
		this.updateParent();
	}

	clickProperty = (item) => {
		this.state.label = item.label;
		this.state.property = item.property;
		this.state.icon = item.icon;
		this.setState();
		this.updateParent();
	}

	updateParent() {
		if (this.props.update) {
			this.props.update('sort', this.state)
		}
	}

	getButtonClasses(config, additional) {
		let classes = "button white-button icon-button select-";
		classes += additional ? additional : '';
		classes += config.visible ? ' open' : '';
		return classes;
	}

	getIconClasses(item) {
		let classes = "fa fa-sort-" + (item.icon || 'amount');
		classes += '-' + (this.state.order == '' ? 'asc' : 'desc');
		return classes;
	}

	render() {
		let config = this.state;

		return (
			<div class="select-menu white-select-menu sort-select">
				<button
					type="button"
					title="Sort Property"
					className={this.getButtonClasses(config, 'label')}
					onClick={this.clickToggle}
				>
					{config.label}
				</button><button
					type="button"
					title="Sort Direction"
					className={this.getButtonClasses(config, 'more')}
					onClick={this.clickOrder}
				>
					<span className={this.getIconClasses(config)}></span>
				</button>
				{
					config.visible && config.items.length > 0
						? <ul class="model-menu"
							onClick={this.clickToggle}
						>
							{this.renderItems(config.items)}
						</ul>
						: ""
				}
			</div>
		);
	}

	renderItems(items) {
		return items.map(item => {
			if (item.if == undefined || item.if) {
				return (
					<li
						className={item.class}
						onClick={() => { this.clickProperty(item) }}
					>
						<a href="javascript:void(0)">
							{item.label}
						</a>
						{this.renderItemIcon(item)}
					</li>
				);
			}
		})
	}

	renderItemIcon(item) {
		if (item.icon) {
			return (
				<i className={this.getIconClasses(item)}></i>
			);
		}
	}
}

LeSort.propTypes = {
	config: propTypes.object.isRequired
};
