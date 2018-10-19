import React, { Component } from "../../../../common/react-vendor";
import "./summary.scss";
import Aux from "../../../../common/widgets/hoc/_Aux";

export default class SummaryContainer extends Component {
  constructor(props) {
    super(props);
  }
  render() {
    return (
      <Aux>
        <div className="le-summary-container le-flex-v-panel">
          <p className="title">Import Templates</p>
          <p className="description">
            Field mapping templates store your import configuration for each
            data object. These templates support all manual and automated data
            import jobs.
          </p>
          <div className="le-flex-h-panel" />
            <div></div>
        </div>
      </Aux>
    );
  }
}
