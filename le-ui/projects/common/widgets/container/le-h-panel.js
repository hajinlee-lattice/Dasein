import React, { Component } from "../../react-vendor";
import "./le-h-panel.scss";
import { LEFT } from "../buttons/le-button";
import { CENTER, RIGHT, TOP, BOTTOM } from "./le-alignments";

class LeHPanel extends Component {
  constructor(props) {
    super(props);
  }
  getHStretch() {
    if (this.props.hstretch && this.props.hstretch.toString() == "true") {
      return "le-h-stretch";
    } else {
      return "";
    }
  }
  getHAlignment() {
    switch (this.props.hAlignment) {
      case LEFT:
        return "";
      case CENTER:
        return "le-pull-h-center";
      case RIGHT:
        return "le-pull-h-right";
      default:
        return "";
    }
  }
  getVStretch() {
    if (this.props.vstretch) {
      return "le-vertical-stretch";
    } else {
      return "";
    }
  }
  getVAlignment() {
    switch (this.props.vAlignment) {
      case TOP:
        return "";
      case CENTER:
        return "le-pull-v-center";
      case BOTTOM:
        return "le-pull-v-bottom";
      default:
        return "";
    }
  }

  getWrap(){
    console.log('WRAP ',this.props.wrap);
    if(this.props.wrap == true){
      return 'le-wrap';
    }else{
      return 'le-nowrap';
    }
  }

  render() {
    const self = this;
    const children = React.Children.map(this.props.children, child => {
      if (React.isValidElement(child)) {
        return React.cloneElement(child, {
          hstretch: self.props.hstretch
        });
      }
    });

    return (
      <div
        className={`le-h-panel ${this.getHStretch()} ${this.getVStretch()} ${this.getHAlignment()} ${this.getVAlignment()} ${this.getWrap()} ${
          this.props.classes ? this.props.classes : ""
        }`}
      >
        {children}
      </div>
    );
    // }
  }
}
export default LeHPanel;
