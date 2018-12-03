import React, { Component } from "../../react-vendor";
import "./le-h-panel.scss";
import { LEFT } from "../buttons/le-button";
import {
  CENTER,
  RIGHT,
  TOP,
  BOTTOM,
  SPACEAROUND,
  SPACEBETWEEN,
  SPACEEVEN
} from "./le-alignments";

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
    console.log(this.props.halignment);
    if (this.props.halignment) {
      switch (this.props.halignment) {
        case LEFT:
          return "le-h-pull-left";
        case CENTER:
          return "le-pull-h-center";
        case RIGHT:
          return "le-pull-h-right";
        case SPACEAROUND:
          return "le-h-space-around";
        case SPACEBETWEEN:
          return "le-h-spaced-between";
        case SPACEEVEN:
          return "le-flex-h-space-even";
        default:
          return "";
      }
    } else {
      return "le-h-pull-left";
    }
  }
  getVStretch() {
    if (this.props.vstretch && this.props.vstretch.toString() == "true") {
      return "le-v-stretch";
    } else {
      return "";
    }
  }
  getVAlignment() {
    switch (this.props.valignment) {
      case TOP:
        return "le-pull-v-top";
      case CENTER:
        return "le-pull-v-center";
      case BOTTOM:
        return "le-pull-v-bottom";
      default:
        return "le-pull-v-top";
    }
  }

  getWrap() {
    console.log("WRAP ", this.props.wrap);
    if (this.props.wrap == true) {
      return "le-wrap";
    } else {
      return "le-nowrap";
    }
  }

  getFlex() {
    if (this.props.flex) {
      return {
        flex: this.props.flex
      };
    }else{
      return {};
    }
  }

  render() {

    return (
      <div
        className={`fill-width ${this.getHStretch()} ${this.getVStretch()}`}
        style={this.getFlex()}
      >
        <div
          className={`le-flex-content sub-container ${this.getHAlignment()} ${this.getVAlignment()} ${this.getVStretch()} ${this.getHStretch()} ${this.getWrap()} ${
            this.props.classesName ? this.props.classesName : ""
          }`}
          style={this.getFlex()}
        >
          {this.props.children}
        </div>
      </div>
    );
    // }
  }
}
export default LeHPanel;
