import React, { Component } from "../../react-vendor";
import debounce from "../utilities/debounce";

import "./le-input-text.scss";

class LeInputText extends Component {
  constructor(props) {
    super(props);
    this.config = props.config ? props.config : {};
    this.clearCallback = this.clearCallback.bind(this);
    this.typedCallback = this.typedCallback.bind(this);
    this.state = { value: "" };
  }

  getPlaceholder() {
    return this.config.placeholder ? this.config.placeholder : "";
  }

  getLabel() {
    if(this.props.config.label){
      return (
        <span className="input-label">
          <span>{this.props.config.label}</span>
        </span>
      );
    }else {
      return null;
    }
  }
  getIcon() {
    if (this.config.icon) {
      return (
        <span className="input-icon">
          <span className={this.config.icon} />
        </span>
      );
    } else {
      return null;
    }
  }

  getClearIcon() {
    if (this.config.clearIcon && this.state.value && this.state.value != '') {
      return (
        <span className="input-icon">
          <span
            className="clear-icon fa fa-times"
            onClick={this.clearCallback}
          />
        </span>
      );
    } else {
      return null;
    }
  }

  clearCallback() {
    this.setState({ value: "" });
    if (this.props.callback) {
      this.props.callback("");
    }
  }
  typedCallback(event) {
    let val = event.target.value;
    this.setState({ value: val });
    if (this.props.callback) {
      debounce(() => {
        return this.props.callback(val);
      }, this.config.debounce ? this.config.debounce : 0);
    }
  }

  render() {
    return (
      <div className="input-icon-wrap">
        {this.getIcon()}
        {this.getLabel()}
        <input
          type="text"
          value={this.state.value}
          onChange={event => {
            this.typedCallback(event);
          }}
          className="input-with-icon"
          placeholder={this.getPlaceholder()}
        />

        {this.getClearIcon()}
      </div>
    );
  }
}

export default LeInputText;
