import './le-menu.scss';
import React, { Component } from 'react';

export default class LeMenu extends Component {
    constructor(props) {
        super(props);
        if(this.props.state){
            this.state = this.props.state
        }else{
            this.state = { opened: false, name: this.props.name }
        }
        
        this.menuClass = `le-menu ${this.props.menuClass}`;
        // this.clickHandler = this.clickHandler.bind(this);
    }

    getImage(image) {
        if (image) {
            const classes = `${image} item-img`;
            return <span className={classes}></span>
        } else {
            return '';
        }
    }
    getLabel(label) {
        if (label) {
            return <span className="label">{label}</span>
        } else {
            return null;
        }
    }
    clickHandler(event) {
        // event.stopPropagation();
        this.setState((prevState, props) => {
            
            if (!prevState.opened) {
                this.menuClass = `le-menu le-menu-show ${props.menuClass}`;
            } else {
                this.menuClass = `le-menu ${props.menuClass}`;
            }

            return {
                opened: !prevState.opened
            }
        })
        if(this.props.callback){
            
            this.props.callback(event, this.props.name);
        }
    }

    render() {

        return (
            <div className={this.menuClass} onClick={(event) => { this.clickHandler(event)}}>
                <div>
                    {this.getImage(this.props.image)}
                    {this.getLabel(this.props.label)}
                </div>
                <div className={`menu-content ${this.props.classNames}`}>
                    {this.props.children}
                </div>
            </div>

        )
    }
}