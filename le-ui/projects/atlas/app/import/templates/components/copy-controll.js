import React, { Component } from "../../../../../common/react-vendor";

class CopyComponent extends Component {
  constructor(props) {
    super(props);
    this.clickHandler = this.clickHandler.bind(this);
  }

  clickHandler(){
    // copyPath(text) {
      console.log(this.props.column, ' -- ',this.props.data);
        let text = this.props.data;
        window.navigator.clipboard.writeText(text).then(
          () => {
              if(this.props.callback){
                  this.props.callback();
              }
            console.log("Async: Copying to clipboard was successful!");
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
