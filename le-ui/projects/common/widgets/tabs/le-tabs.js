import './le-tabs.scss';
import React, { Component } from 'react';

export default class LeTabs extends Component {
    constructor(props) {
        super(props);
    }

    render() {
        this.tabsClass=`tabs ${this.props.justified == true ? 'justified' : ''}`;
        return (
            <div className={this.tabsClass}>
                <ul>
                    {this.props.children}
                </ul>
            </div>
        )
    }
}