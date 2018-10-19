import React, { Component } from "../../react-vendor";
import "../layout/layout.scss";
import "./le-link.scss";

export const LEFT = "left";
export const RIGHT = "right";

export default class LeLink extends Component {
  constructor(props) {
    super(props);
    console.log('PROPS ==> ', props);
  }

  getIcon(position) {
    if (
      this.props.config.icon &&
      this.props.config.icon != "" &&
      position == this.props.config.iconside
    ) {
      const classes = `${this.props.config.icon} le-link-icon`;
      return <i className={classes} />;
    } else {
      return null;
    }
  }
  render() {
    
    let customClasses = this.props.config.classes ? this.props.config.classes : '';
    return (

      <div className={`le-flex-h-panel center-v le-link-container ${customClasses}`}>
        <div className="wrapper" onClick={this.props.callback}>
          {this.getIcon(LEFT)}
          <span className="le-link">{this.props.config.label}</span>
          {this.getIcon(RIGHT)}
        </div>
      </div>
    );
  }
}
