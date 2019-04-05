import React, { Component } from "common/react-vendor";
import propTypes from "prop-types";
import "./le-refine.scss";

export default class LeRefine extends Component {
  constructor(props) {
    super(props);
  }

  render() {
    return (
      <div class="le-refine">
        refine
      </div>
    );
  }
}

LeRefine.propTypes = {
  config: propTypes.object.isRequired
};
