import React, { Component } from "../../../react-vendor";

class CopyComponent extends Component {
  constructor(props) {
    super(props);
    this.clickHandler = this.clickHandler.bind(this);
  }

  clickHandler() {
    window.navigator.clipboard.writeText(this.props.data).then(
      () => {
        if (this.props.callback) {
          this.props.callback();
        }
        console.log("Async: Copying to clipboard was successful!");
      },
      err => {
        console.error("Async: Could not copy text: ", err);
      }
    );
  }
  render() {
    return (
      <li
        className="le-table-cell-icon le-table-cell-icon-actions initially-hidden"
        title={`${this.props.title ? this.props.title: 'Copy'}`}
        onClick={this.clickHandler}
      >
        <i className="fa fa-files-o" />
      </li>
    );
  }
}

export default CopyComponent;
