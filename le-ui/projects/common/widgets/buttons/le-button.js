import React, { Component } from "../../react-vendor";
import PropTypes from "prop-types";
import "./buttons.scss";

export const LEFT = "left";
export const RIGHT = "right";

export default class LeButton extends Component {
  constructor(props) {
    super(props);
    this.state = this.props.state || { disabled: false };
    this.clickHandler = this.clickHandler.bind(this);
  }

  clickHandler() {
    this.props.callback(this.state);
  }

  createIcon() {
    return <span className={this.props.config.icon} />;
  }

  getIconByPosition(position) {
    switch (position) {
      case LEFT:
        if (
          !this.props.config.iconside ||
          position == this.props.config.iconside
        )
          return this.createIcon();

        break;
      case RIGHT:
        if (position == this.props.config.iconside) return this.createIcon();

        break;
    }
  }
  getIcon(position) {
    if (this.props.config.icon) {
      return this.getIconByPosition(position);
    } else {
      return null;
    }
  }
  getLabel() {
    if (this.props.config.lable && this.props.config.lable !== "") {
      return <span className="le-button-title">{this.props.config.lable}</span>;
    } else {
      return "";
    }
  }
  getClasses() {
      
    let classes = `button ${
      this.props.config.classNames ? this.props.config.classNames : ""
    } ${(this.props.disabled || this.state.disabled) ? 'disabled': ''}`;
    console.log("THE CLASSES ", classes);
    return classes;
  }
  render() {
    return (
      <button
        onClick={this.clickHandler}
        disabled={this.props.disabled}
        className={this.getClasses()}
      >
        {this.getIcon(LEFT)}
        {this.getLabel()}
        {this.getIcon(RIGHT)}
      </button>
    );
  }
}
LeButton.propTypes = {
  lable: PropTypes.string,
  classNames: PropTypes.array,
  callback: PropTypes.func
};
