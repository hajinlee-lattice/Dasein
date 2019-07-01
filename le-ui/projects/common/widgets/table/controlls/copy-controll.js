import React, { Component } from "../../../react-vendor";
import './copy-control.scss';
class CopyComponent extends Component {
  constructor(props) {
    super(props);
    this.clickHandler = this.clickHandler.bind(this);
  }

  clickHandler(event){
        let text = this.props.data;
        window.navigator.clipboard.writeText(text).then(
          () => {
              if(this.props.callback){
                  this.props.callback(event);
              }
          },
          err => {
            console.error("Async: Could not copy text: ", err);
          }
        );
  }
  getData(){
    if(this.props.data){
      return (<span>{this.props.data}</span>)
    }else{
      return null;
    }
  }
  render() {
    console.log('TEST');
    return (
      <li
        className="le-table-cell-icon le-table-cell-icon-actions initially-hidden"
        title="Copy Link"
        onClick={this.clickHandler}
      > 
        {this.getData()}
        <i className="fa fa-files-o le-copy-control" />
      </li>
    );
  }
}

export default CopyComponent;
