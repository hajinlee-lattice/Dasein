import React, { Component } from "common/react-vendor";
import propTypes from "prop-types";
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
    this.props.callback(this.props.name, this.state);
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
    if (this.props.config.label && this.props.config.label !== "") {
      return <span className="le-button-title">{this.props.config.label}</span>;
    } else {
      return "";
    }
  }

  getClasses() {
    let classes = `button ${
      this.props.config.classNames ? this.props.config.classNames : ""
    } ${(this.props.disabled || this.state.disabled == true) ? 'disabled': ''}`;
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
  name: propTypes.string.isRequired,
  label: propTypes.string,
  classNames: propTypes.array,
  callback: propTypes.func
};
