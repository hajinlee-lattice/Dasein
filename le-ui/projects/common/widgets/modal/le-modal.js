import React, { Component, ReactDOM } from "common/react-vendor";
import './le-modal.scss';

import LeButton from '../buttons/le-button';
// import { store, injectAsyncReducer } from 'store';
import { actions, reducer } from './le-modal.redux';

const modalRoot = document.getElementById('le-modal');

export default class LeModal extends Component {

    constructor(props) {
        super(props);
        this.clickHandler = this.clickHandler.bind(this);
        console.log('MODAL PROPS ==> ', props);
        this.state = { opened: false };
        if(this.props.config.injectAsyncReducer){
            this.props.config.injectAsyncReducer(this.props.config.store, 'le-modal', reducer);
        }
        
        this.unsubscribe = this.props.config.store.subscribe(() => {
            const data = this.props.config.store.getState()['le-modal'];
            this.setState({
                opened: data.opened,
                callbackFn: data.callbackFn,
                templateFn: data.templateFn
            });
        });
        this.clickHandler = this.clickHandler.bind(this);
    }

    clickHandler(action) {
        switch(action){
            case 'close':
            actions.toggleModal(this.props.config.store);
            break;
        }
        if (this.state.callbackFn) {
            this.state.callbackFn(action);
        }
    }
    getTitle() {
        return this.props.config.title ? this.props.config.title : '';
    }

    getClassName() {
    }

    getModalUI() {
        console.log('Render modal');
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
                        {this.state.templateFn()}
                    </div>

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
                </div>

            </div>
        );
    }

    render() {
        if (this.state.opened == true) {
            let modal = this.getModalUI();
            return ReactDOM.createPortal(
                modal, modalRoot
            );
        } else {
            return null;
        }
    }
}