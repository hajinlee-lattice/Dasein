import React, { Component } from "../../react-vendor";
import "./le-v-panel.scss";
import {
  LEFT,
  CENTER,
  TOP,
  BOTTOM,
  SPACEAROUND,
  SPACEBETWEEN,
  SPACEEVEN
} from "./le-alignments";
import { RIGHT } from "../link/le-link";

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
        case LEFT:
          return "le-flex-pull-left";
        case CENTER:
          return "le-flex-h-centered";
        case RIGHT:
          return "le-flex-pull-right";
      }
    } else {
      return "le-flex-pull-left";
    }
  }
  getVAlignment() {
    console.log(this.props.valignment);
    if (this.props.valignment) {
      switch (this.props.valignment) {
        case TOP:
          return "le-flex-v-top";
        case CENTER:
          return "le-flex-v-centered";
        case BOTTOM:
          return "le-flex-v-bottom";
        case SPACEAROUND:
          return "le-flex-v-space-around";
        case SPACEBETWEEN:
          return "le-flex-v-space-between";
        case SPACEEVEN:
          return "le-flex-v-space-even";
      }
    } else {
      return "le-flex-v-top";
    }
  }

  render() {

    return (
      <div
        className={`fill-height ${this.getVStretch()} ${this.getHStretch()}`}
      >
        <div
          className={`le-flex-content sub-container ${this.getHAlignment()} ${this.getVAlignment()} ${this.getVStretch()} ${this.getHStretch()}`}
        >
          {this.props.children}
        </div>
      </div>
    );
  }
}

export default LeVPanel;
