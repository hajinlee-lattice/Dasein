import React, { Component } from 'common/react-vendor';
import {CENTER} from '../le-alignments';
import './le-card.scss';

class LeCardBody extends Component {
    constructor(props) {
        super(props);
    }
    render() {

        return (
            <div className={`${"le-card-body"} ${this.props.contentAlignment==CENTER ? 'content-centered': ''} ${this.props.classNames ? this.props.classNames : ''}`}>
                {this.props.children}
            </div>
        );
    }
}

export default LeCardBody;