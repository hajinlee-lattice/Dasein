import React, { Component } from "../../react-vendor";
import "./le-pagination.scss";
import LeButton from "../buttons/le-button";

export default class LePagination extends Component {
	constructor(props) {
		super(props);

		this.clickHandler = this.clickHandler.bind(this);
		this.state = { current: props.start };
	}

	init() {
		this._total = this.props.total;
		this._startPage = this.props.start;
		this._perPage = this.props.perPage;
		this._numPages = this._total / this._perPage;
		let tmp = this._total % this._perPage;
		console.log(
			"> pagination init",
			tmp,
			this._numPages,
			this._total,
			this._perPage
		);
		if (tmp > 0) {
			this._numPages = Math.ceil(this._numPages);
		}
	}

	getFromTo(page) {
		let from = (page - 1) * this._perPage;
		let to = page * this._perPage;
		let current = this.state.current;
		if (to > this._total) {
			to = this._total;
		}
		return { from, to, current };
	}

	componentDidMount() {
		this.clickHandler("current");
	}

	clickHandler(direction) {
		switch (direction) {
			case "current":
				this.setState({ current: this.state.current }, () => {
					this.props.callback(this.getFromTo(this.state.current));
				});
				break;
			case "first":
				this.setState({ current: 1 }, () => {
					this.props.callback(this.getFromTo(this.state.current));
				});
				break;
			case "next":
				if (this.state.current < this._numPages) {
					this.setState({ current: this.state.current + 1 }, () => {
						this.props.callback(this.getFromTo(this.state.current));
					});
				}
				break;

			case "prev":
				if (this.state.current > 1) {
					this.setState({ current: this.state.current - 1 }, () => {
						this.props.callback(this.getFromTo(this.state.current));
					});
				}

				break;
			case "last":
				this.setState({ current: this._numPages }, () => {
					this.props.callback(this.getFromTo(this.state.current));
				});
				break;
		}
	}
	render() {
		this.init();
		if (this._numPages > 1) {
			return (
				<div
					className={`pd-pagination ${
						this.props.className ? this.props.className : ""
					}`}
				>
					<LeButton
						name="borderless-former"
						callback={() => {
							this.clickHandler("first");
						}}
						disabled={false}
						config={{
							classNames: "borderless-button",
							icon: "fa fa-angle-double-left"
						}}
					/>
					<LeButton
						name="borderless-firts"
						callback={() => {
							this.clickHandler("prev");
						}}
						disabled={false}
						config={{
							classNames: "borderless-button",
							icon: "fa fa-angle-left"
						}}
					/>
					<span className="pd-pagination-center">
						<span className="pd-pagination-pagenum">
							{this.state.current}
						</span>
						<span>/</span>
						<span className="pd-pagination-pagetotal">
							{this._numPages}
						</span>
					</span>

					<LeButton
						name="borderless-next"
						callback={() => {
							this.clickHandler("next");
						}}
						disabled={false}
						config={{
							classNames: "borderless-button",
							icon: "fa fa-angle-right"
						}}
					/>

					<LeButton
						name="borderless-firts"
						callback={() => {
							this.clickHandler("last");
						}}
						disabled={false}
						config={{
							classNames: "borderless-button",
							icon: "fa fa-angle-double-right"
						}}
					/>
				</div>
			);
		} else {
			return null;
		}
	}
}
