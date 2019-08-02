import React, { Component, ReactDOM } from "common/react-vendor";
import propTypes from "prop-types";
import "./le-banner.scss";
import { actions, reducer } from "./le-banner.redux";

export default class LeBanner extends Component {
	constructor(props) {
		super(props);
		this.clickHandler = this.clickHandler.bind(this);
		this.state = { store: props.store, bannersList: [] };
		if (this.props.injectAsyncReducer) {
			this.props.injectAsyncReducer(
				props.store,
				props.reduxstate,
				reducer
			);
		}

		this.unsubscribe = props.store.subscribe(() => {
			const data = props.store.getState()[props.reduxstate];
			let listCopy = data.bannersList || [];
			this.setState({
				bannersList: listCopy
			});
		});
		this.clickHandler = this.clickHandler.bind(this);
	}
	getCount(banner) {
		if (banner.count > 1) {
			return <span className="le-banner-count">{banner.count}</span>;
		}
	}

	componentDidMount() {
		this.bannerRoot = document.getElementById("react-banner-container");
	}

	clickHandler(banner) {
		actions.closeBanner(this.props.store, banner);
	}

	getMessage(message) {
		let msg = <p>{message}</p>;
		return msg;
	}

	getBunners() {
		let bannersUI = [];
		this.state.bannersList.forEach(banner => {
			bannersUI.push(
				<div className={`${"le-banner"} ${banner.type}`}>
					{this.getCount(banner)}
					<div className="le-banner-icon-container">
						<i className="le-banner-icon" />
					</div>
					<div className="le-banner-content">
						<div className="le-banner-title">{banner.title}</div>
						<div className="le-banner-message">
							{this.getMessage(banner.message)}
						</div>
					</div>
					<div className="le-banner-close-container">
						<i
							onClick={() => {
								this.clickHandler(banner);
							}}
						>
							X
						</i>
					</div>
				</div>
			);
		});
		return bannersUI;
	}

	getBannerUI() {
		return <div className="le-banners-container">{this.getBunners()}</div>;
	}

	render() {
		if (this.state.bannersList.length > 0) {
			let banner = this.getBannerUI();
			return ReactDOM.createPortal(banner, this.bannerRoot);
		} else {
			return null;
		}
	}
}

LeBanner.propTypes = {
	config: propTypes.objectOf(
		propTypes.shape({
			title: propTypes.string,
			message: propTypes.string
		})
	)
};
