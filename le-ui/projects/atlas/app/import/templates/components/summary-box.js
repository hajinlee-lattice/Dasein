import React, { Component } from "../../../../../common/react-vendor";
import "./summary-box.scss";

export default class SummaryBox extends Component {
	constructor(props) {
		super(props);
	}
	getValueWithComma(value) {
		return value
			? value.toString().replace(/(\d)(?=(\d{3})+(?!\d))/g, "$1,")
			: 0;
	}
	getCount() {
		if (this.props.loading) {
			return <i class="fa fa-spinner fa-spin fa-fw" />;
		} else {
			return <span>{this.getValueWithComma(this.props.count)}</span>;
		}
	}
	componentDidMount() {}
	render() {
		return (
			<div className="le-flex-h-panel flex-content">
				<div className="le-summary-box le-flex-v-panel">
					<div className="title">
						{this.props.name}
						<p className="asidecount pull-right">
							{this.props.asidename
								? this.getValueWithComma(this.props.asidecount)
								: ""}
						</p>
					</div>
					<div className="count">
						{this.getCount()}
						<p className="asidename pull-right">
							{this.props.asidename}
						</p>
					</div>
				</div>
			</div>
		);
	}
}
