import React, { Component, ReactDOM } from "common/react-vendor";
import propTypes from "prop-types";
import './le-banner.scss';
import { TYPE_INFO } from './le-banner.utils';
import { actions, reducer } from './le-banner.redux';



export default class LeBanner extends Component {

    constructor(props) {
        super(props);
        this.clickHandler = this.clickHandler.bind(this);
        this.state = { store: props.store, open: false, config: {}, bannersList: [] };
        if (this.props.injectAsyncReducer) {
            this.props.injectAsyncReducer(props.store, props.reduxstate, reducer);
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

    componentDidMount() {
        this.bannerRoot = document.getElementById('react-banner-container')
    }

    clickHandler(banner) {
        actions.closeBanner(this.props.store, banner);
    }

    getBunners() {
        let bannersUI = [];
        this.state.bannersList.forEach(banner => {
            bannersUI.push(
                (<div className={`${"le-banner"} ${banner.type}`}>
                    <div className="le-banner-icon-container">
                        <i className="le-banner-icon"></i>
                    </div>
                    <div className="le-banner-content">
                        <div className="le-banner-title">{banner.title}</div>
                        <div className="le-banner-message">
                            {banner.message}</div>
                    </div>
                    <div className="le-banner-close-container">
                        <i onClick={() => { this.clickHandler(banner) }}>X</i>
                    </div>
                </div>)
            );
        });
        return bannersUI;
    }

    getBannerUI() {
        return (
            <div className="le-banners-container">
                {this.getBunners()}
            </div>
        );
    }

    render() {
        if (this.state.bannersList.length > 0) {
            let banner = this.getBannerUI();
            return ReactDOM.createPortal(
                banner, this.bannerRoot
            );
        } else {
            return null;
        }
    }
}



LeBanner.propTypes = {
    config: propTypes.objectOf(propTypes.shape({
        title: propTypes.string,
        message: propTypes.string
    }))
};