import React, { Component } from "common/react-vendor";
import propTypes from "prop-types";
import "./table.scss";

export default class CellContent extends Component {
    constructor(props) {
        super(props);
    }
    getContent() {
        if (this.props.mask) {
            let t = this.props.mask(this.props.value);
            t = "" + t;
            return t;
        } else {
            return this.props.value;
        }
    }
    render() {
        if (this.props.editing === true) {
            return null;
        } else {
            let content = this.getContent();
            return (
                <li className="le-table-cell-content">
                    <span title={content}>{content}</span>
                </li>
            );
        }
    }
}

CellContent.propTypes = {
    value: propTypes.any.isRequired,
    mask: propTypes.func
};
