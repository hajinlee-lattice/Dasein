import React, { Component } from "../../../react-vendor";

class CopyComponent extends Component {
  constructor(props) {
    super(props);
    this.clickHandler = this.clickHandler.bind(this);
  }

  clickHandler(event){
    // copyPath(text) {
        let text = this.props.data;
        window.navigator.clipboard.writeText(text).then(
          () => {
              if(this.props.callback){
                  this.props.callback(event);
              }
            // console.log("Async: Copying to clipboard was successful!");
          },
          err => {
            console.error("Async: Could not copy text: ", err);
          }
        );
    //   }
  }
  render() {
    return (
      <li
        className="le-table-cell-icon le-table-cell-icon-actions initially-hidden"
        title="Copy Link"
        onClick={this.clickHandler}
      >
        <i className="fa fa-files-o" />
      </li>
    );
  }
}

export default CopyComponent;
