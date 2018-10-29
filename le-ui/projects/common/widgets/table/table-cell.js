import React, { Component } from "../../react-vendor";
import PropTypes from "prop-types";
import "./table.scss";

export default class LeGridCell extends Component {
  constructor(props) {
    super(props);
    this.state = { editing: false };
    this.editable = this.props.editable ? this.props.editable : false;
    this.toogleEdit = this.toogleEdit.bind(this);
  }

  update(){
    this.setState({ editing: !this.state.editing });
  }

  toogleEdit() {
    if (this.editable) {
      // this.update();
      this.setState({ editing: !this.state.editing });

    }
  }

  render() {
    const { children } = this.props;
    const newProps = {};
    Object.keys(this.props).forEach(prop => {
      if(prop != 'children'){
        newProps[prop] = this.props[prop];
      }
    });
    newProps.toogleEdit = this.toogleEdit;
    newProps.editing = this.state.editing;
    var childrenWithProps = React.Children.map(children, child =>
      React.cloneElement(child, newProps)
    );

    let span = `le-table-cell le-table-col-span-${this.props.colSpan} cell-${
      this.props.row
    }-${this.props.col} ${this.props.colName}`;
    let externalFormatting = "";
    if (this.props.config && this.props.config.formatter) {
      externalFormatting = this.props.config.formatter(this.props.rowData);
    }
    let format = `${span} ${externalFormatting}`;
    return <ul className={format}><div>{childrenWithProps}</div></ul>;
  }
}

LeGridCell.propTypes = {};
