import React, { Component } from "react";
import PropTypes from "prop-types";

import "./button-actions.scss";

export default class LeButtonActions extends Component {
  constructor(props) {
    super(props);
    this.state = this.props.state || { disabled: false };
    this.clickHandler = this.clickHandler.bind(this);
  }
  clickHandler() {
    if (this.props.callback) {
      this.props.callback("Send the action");
    }
  }

  render() {
    return (
      <div className="button-actions-container">
        <button className="button-actions" onClick={this.clickHandler}>
          Test
        </button>
        <button className="button-actions actions-drop">
          <span className={this.props.config.image} />
        </button>
      </div>
    );
  }
}
LeButtonActions.propTypes = {};
