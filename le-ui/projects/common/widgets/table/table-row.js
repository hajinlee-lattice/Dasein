import React, { Component } from "../../react-vendor";
import "./table.scss";

export default class LeGridRow extends Component {
  constructor(props) {
    super(props);
  }


  render() {
    let rowClass = `le-table-row row-${this.props.index} ${this.rowClasses ? this.rowClasses : ''}`;
    let externalFormatting = ''
    if(this.props.formatter){
      externalFormatting = this.props.formatter(this.props.rowData);
    } 
    let format = `${rowClass} ${externalFormatting ? externalFormatting: ''}`;
    // console.log('FORM ', format);
    return (
      <div className={format}>
        {this.props.children}
      </div>
    );
  }
}