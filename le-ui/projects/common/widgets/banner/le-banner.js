import React, { Component, ReactDOM } from "common/react-vendor";
import propTypes from "prop-types";
import './le-banner.scss';
import { TYPE_INFO } from './le-banner.utils';
import { actions, reducer } from './le-banner.redux';



export default class LeBanner extends Component {

    constructor(props) {
        super(props);
        this.clickHandler = this.clickHandler.bind(this);
        this.state = { store: props.store, open: false, config: {} };
        if (this.props.injectAsyncReducer) {
            this.props.injectAsyncReducer(props.store, props.reduxstate, reducer);
        }

        this.unsubscribe = props.store.subscribe(() => {
            const data = props.store.getState()[props.reduxstate];
            this.setState({
                open: data.open,
                title: data.config.title ? data.config.title : '',
                type: data.config.type ? data.config.type : TYPE_INFO,
                message: data.config.message ? data.config.message : 'No Message Provided'
            });
        });
        this.clickHandler = this.clickHandler.bind(this);
    }

    componentDidMount() {
        this.bannerRoot = document.getElementById('react-banner-container')
        console.log('BANNER ', this.bannerRoot);
    }

    clickHandler(action) {
        actions.closeBanner(this.props.store);
    }

    getBannerUI() {
        return (

            <div className={`${"le-banner"} ${this.state.type}`}>
                <div className="le-banner-icon-container">
                    <i className="le-banner-icon"></i>
                </div>
                <div className="le-banner-content">
                    <div className="le-banner-title">{this.state.title}</div>
                    <div className="le-banner-content">
                        {this.state.message}</div>
                </div>
                <div className="le-banner-close-container">
                    <i onClick={this.clickHandler}>X</i>
                </div>
            </div>
        );
    }

    render() {
        if (this.state.open == true) {
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