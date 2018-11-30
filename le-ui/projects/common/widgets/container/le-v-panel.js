import React, { Component } from "../../react-vendor";
import "./le-v-panel.scss";

class LeVPanel extends Component {
  constructor(props) {
    super(props);
  }

  getVStretch() {
    if (this.props.vstretch && this.props.vstretch.toString() == "true") {
      return "le-v-stretch";
    } else {
      return "";
    }
  }

  getHStretch() {
    if (this.props.hstretch && this.props.hstretch.toString() == "true") {
      return "le-h-stretch";
    } else {
      return "";
    }
  }
  getHAlignment() {
    console.log(this.props.halignment);
    if (this.props.halignment) {
      switch (this.props.halignment) {
        case "left":
          return "le-flex-pull-left";
        case "center":
          return "le-flex-h-centered";
        case "right":
          return "le-flex-pull-right";
      }
    } else {
      return "le-flex-pull-left";
    }
  }
  getVAlignment(){
    console.log(this.props.valignment);
    if (this.props.valignment) {
      switch (this.props.valignment) {
        case "top":
          return "le-flex-v-top";
        case "center":
          return "le-flex-v-centered";
        case "bottom":
          return "le-flex-v-bottom";
      }
    } else {
      return "le-flex-v-top";
    }
  }

  render() {
    const self = this;
    const children = React.Children.map(this.props.children, child => {
      if (React.isValidElement(child)) {
        return React.cloneElement(child, {
          vstretch: self.props.vstretch,
          hstretch: self.props.hstretch,
          halignment: self.props.halignment ? self.props.halignment : child.props.halignment,
          valignment: self.props.valignment ? self.props.valignment : child.props.valignment 
        });
      }
    });
    
    return (
      <div
        className={`fill-height-or-more ${this.getVStretch()} ${this.getHStretch()}`}
      >
        <div
          className={`le-flex-content sub-container ${this.getHAlignment()} ${this.getVAlignment()} ${this.getVStretch()} ${this.getHStretch()}`}
        >
          {children}
        </div>
      </div>
    );
  }
}

export default LeVPanel;
