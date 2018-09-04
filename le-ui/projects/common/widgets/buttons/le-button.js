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
    getIcon() {
        console.log('this.props.config.image', this.props.config.image);
        if(this.props.config.image){
            return <span className={this.props.config.image}></span>
        }else{
            return null;
        }
    }
    getLabel() {
        if(this.props.config.lable && this.props.config.lable !== ''){
            return this.props.config.lable;
        }else{
            return '';
        }
    }

    render() {
        const enabled = !this.state.disabled;
        return (
            <button
                onClick={this.clickHandler}
                disabled={this.props.disabled}
                className={this.config.classNames.join(' ')}>
                {this.getLabel()}
                {this.getIcon()}
            </button>
        );
    }
}
LeButton.propTypes = {
    lable: PropTypes.string,
    classNames: PropTypes.array,
    callback: PropTypes.func
}