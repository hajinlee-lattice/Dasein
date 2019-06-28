import React, { Component } from "../../react-vendor";
import debounce from "../utilities/debounce";

import "./le-simple-input-text.scss";

class LeSimpleInputText extends Component {
  constructor(props) {
    super(props);
    // this.config = props.config ? props.config : {};
    // this.clearCallback = this.clearCallback.bind(this);
    this.typedCallback = this.typedCallback.bind(this);
    this.state = { value: "" };
  }

  getPlaceholder() {
    return this.props.config.placeholder ? this.props.config.placeholder : "";
  }

  getLabel() {
    if(this.props.config.label){
      return (
        <span className="le-simple-input-label">
          <span>{this.props.config.label}</span>
        </span>
      );
    }else {
      return null;
    }
  }
  getIcon() {
    
  }

  getClearIcon() {
    
  }

  
  typedCallback(event) {
    let val = event.target.value;
    this.setState({ value: val });
    this.props.callback(val);
  }

  render() {
    return (
      <div className="le-simple-text-container">
        {this.getLabel()}
        <input
          type="text"
          value={this.state.value}
          onChange={event => {
            this.typedCallback(event);
          }}
          className="le-simple-input"
          placeholder={this.getPlaceholder()}
        />
      </div>
    );
  }
}

export default LeSimpleInputText;
