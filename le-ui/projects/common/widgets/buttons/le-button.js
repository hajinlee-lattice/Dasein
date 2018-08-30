import React, { Component } from 'react';
import PropTypes from 'prop-types';

import './buttons.scss';

export default class LeButton extends Component {
    constructor(props) {
        super(props);
        this.config = this.props.config || {
            lable: "Click me!",
            classNames: []
        };

        this.state = this.props.state || {disabled : false};
        this.clickHandler = this.clickHandler.bind(this);
    }
    clickHandler(){
        this.props.callback(this.state);
    }

    render() {
        const enabled = !this.state.disabled;
        return (
            <button
                onClick={this.clickHandler}
                disabled={this.props.disabled}
                className={this.config.classNames.join(' ')}>
                {this.props.lable}
            </button>
        );
    }
}
LeButton.propTypes = {
    lable: PropTypes.string,
    classNames: PropTypes.array,
    callback: PropTypes.func
}