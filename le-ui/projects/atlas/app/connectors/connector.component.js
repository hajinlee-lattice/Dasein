import React, { Component } from "common/react-vendor";
import LeCard from 'common/widgets/container/card/le-card';
import LeCardImg from 'common/widgets/container/card/le-card-img';
import LeCardBody from 'common/widgets/container/card/le-card-body';
import { CENTER } from 'common/widgets/container/le-alignments';
export default class Connector extends Component {
    constructor(props) {
        super(props);
        this.connectorClickHandler = this.connectorClickHandler.bind(this);
    }
    connectorClickHandler() {
        if (this.props.clickHandler) {
            this.props.clickHandler(this.props.name);
        }
    }
    render() {
        return (
            <LeCard classNames={`${"le-connector"} ${this.props.name} ${this.props.classNames ? this.props.classNames : ''}`} clickHandler={this.connectorClickHandler}>
                <LeCardImg src={this.props.config.img} />
                <LeCardBody contentAlignment={CENTER}>
                    <p>{this.props.config.text}</p>
                </LeCardBody>
            </LeCard>
        );
    }
}