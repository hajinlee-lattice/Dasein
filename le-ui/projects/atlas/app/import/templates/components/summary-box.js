import React, { Component } from "../../../../../common/react-vendor";
import "./summary-box.scss";

export default class SummaryBox extends Component {
  constructor(props) {
    super(props);
  }

  getCount() {
    if (this.props.loading) {
      return <i class="fa fa-spinner fa-spin fa-fw" />;
    } else {
      return (
          <p>{this.props.count}</p>
      );
    }
  }
  componentDidMount() {}
  render() {
    return (
      <div className="le-flex-h-panel flex-content">
        <div className="le-summary-box le-flex-v-panel">
          <p className="title">{this.props.name}</p>
          {this.getCount()}
        </div>
      </div>
    );
  }
}
