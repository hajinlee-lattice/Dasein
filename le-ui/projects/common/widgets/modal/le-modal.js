import React, { Component, ReactDOM } from "common/react-vendor";
import './le-modal.scss';

import LeButton from '../buttons/le-button';

const modalRoot = document.getElementById('le-modal');

export default class LeModal extends Component {

    constructor(props) {
        super(props);
        this.clickHandler = this.clickHandler.bind(this);
    }

    clickHandler(action) {
        this.props.callback(action);
    }
    getTitle() {
        return this.props.title ? this.props.title : '';
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
                        {this.props.template()}
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
                                label: "CANCEL",
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
        if (this.props.opened == true) {
            let modal = this.getModalUI();
            return ReactDOM.createPortal(
                modal, modalRoot
            );
        } else {
            return null;
        }
    }
}