import React, { Component, ReactDOM } from "common/react-vendor";
import './le-modal.scss';

export const INFO = 'INFO';
export const WARNING = 'WARNING';
export const ERROR = 'ERROR';

import LeButton from '../buttons/le-button';
// import { store, injectAsyncReducer } from 'store';
import { actions, reducer } from './le-modal.redux';

const modalRoot = document.getElementById('le-modal');

export default class LeModal extends Component {

    constructor(props) {
        super(props);
        this.clickHandler = this.clickHandler.bind(this);
        this.state = {store: props.store, open: false, config: {}};
        if (this.props.injectAsyncReducer) {
            this.props.injectAsyncReducer(props.store, 'le-modal', reducer);
        }

        this.unsubscribe = props.store.subscribe(() => {
            const data = props.store.getState()['le-modal'];
            this.setState({
                open: data.open,
                callbackFn: data.config.callback,
                templateFn: data.config.template,
                title: data.config.title,
                hideFooter: data.config.hideFooter ? data.config.hideFooter : false
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
        if(typeof this.state.title === 'function'){
            return this.state.title();
        }else {
            return '';
        }
    }

    hideFooter() {
        return this.state.hideFooter;
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
                            classNames: "blue-button"
                        }}
                        callback={() => { return this.clickHandler('ok') }}
                    />
                    <LeButton
                        name={`${"modal-cancel"}`}
                        config={{
                            label: "Cancel",
                            classNames: "gray-button"
                        }}
                        callback={() => { return this.clickHandler('close') }}
                    />
                </div>
            );
        }
    }

    getTemplate(){
        if(this.state.templateFn){
            return this.state.templateFn();
        }else{
            return null;
        }
    }
    getClassName() {
    }

    getModalUI() {
        return (
            <div className="le_modal opened">
                <div className="le-modal-content">
                    <div className="le-modal-header">
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