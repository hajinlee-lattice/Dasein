import React, { Component, ReactDOM } from "common/react-vendor";
import propTypes from "prop-types";
import './le-modal.scss';
import { TYPE_ERROR, TYPE_INFO, TYPE_SUCCESS, TYPE_WARNING, MEDIUM_SIZE } from './le-modal.utils';


import LeButton from '../buttons/le-button';
import { actions, reducer } from './le-modal.redux';
import { isBoolean } from "util";
const modalRoot = document.getElementById('le-modal');

export default class LeModal extends Component {

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
                callbackFn: data.config.callback,
                templateFn: data.config.template,
                title: data.config.title,
                hideFooter: data.config.hideFooter ? data.config.hideFooter : false,
                type: data.config.type ? data.config.type : '',
                oneButton: data.config.oneButton ? data.config.oneButton : false,
                size: data.config.size ? data.config.size : MEDIUM_SIZE
            });
        });
        this.clickHandler = this.clickHandler.bind(this);
    }

    clickHandler(action) {
        switch (action) {
            case 'close':
                actions.closeModal(this.props.store);
                break;
        }
        if (this.state.callbackFn) {
            this.state.callbackFn(action);
        }
    }

    getTitle() {
        if (typeof this.state.title === 'function') {
            return this.state.title();
        } else {
            return '';
        }
    }

    hideFooter() {
        return this.state.hideFooter;
    }

    getConfirmClass() {
        let buttonClass = 'blue-button';
        switch (this.state.type) {
            case TYPE_ERROR:
                buttonClass = 'red-button';
                break;
            case TYPE_INFO:
                buttonClass = 'blue-button';
                break;
            case TYPE_SUCCESS:
                buttonClass = 'green-button';
                break;
            case TYPE_WARNING:
                buttonClass = 'orange-button';
                break;
        }
        return buttonClass;
    }

    getCancelButton() {
        if (!this.state.oneButton) {
            return (<LeButton
                name={`${"modal-cancel"}`}
                config={{
                    label: "Cancel",
                    classNames: "gray-button"
                }}
                callback={() => { return this.clickHandler('close') }}
            />);
        } else {
            return null;
        }
    }
    getFooter() {
        if (this.state.hideFooter) {
            return null;
        } else {
            return (
                <div className="le-modal-footer">
                    <LeButton
                        name={`${"modal-ok"}`}
                        config={{
                            label: "Done",
                            classNames: this.getConfirmClass()
                        }}
                        callback={() => { return this.clickHandler('ok') }}
                    />
                    {this.getCancelButton()}
                </div>
            );
        }
    }

    getTemplate() {
        if (this.state.templateFn) {
            return this.state.templateFn();
        } else {
            return null;
        }
    }
    getClassName() {
    }

    getModalUI() {
        console.log('MODAL STATE ', this.state);
        return (
            <div className="le_modal opened">
                <div className={`${"le-modal-content"} ${this.state.size}`}>
                    <div className={`${"le-modal-header"} ${this.state.type ? this.state.type : ''}`}>
                        <span className="close" onClick={() => {
                            this.clickHandler('close');
                        }}>&times;</span>
                        <div className="le-title-container">
                            <span className="ico-container">
                                <i className="title-icon"></i>
                            </span>
                            <p className="le-title" title={this.getTitle()}>
                                {this.getTitle()}
                            </p>
                        </div>
                    </div>
                    <div className="le-modal-body">
                        {this.getTemplate()}
                    </div>
                    {this.getFooter()}
                </div>
            </div>
        );
    }

    render() {
        if (this.state.open == true) {
            let modal = this.getModalUI();
            return ReactDOM.createPortal(
                modal, modalRoot
            );
        } else {
            return null;
        }
    }
}



LeModal.propTypes = {
    config: propTypes.objectOf(propTypes.shape({
        open: propTypes.bool,
        callback: propTypes.func,
        template: propTypes.func,
        title: propTypes.func,
        hideFooter: propTypes.bool,
        type: propTypes.string,
        oneButton: propTypes.bool,
        size: propTypes.string
    }))
};