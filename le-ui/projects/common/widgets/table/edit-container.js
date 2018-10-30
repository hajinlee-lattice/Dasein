import React, { Component } from "../../react-vendor";
import PropTypes from "prop-types";
import "./table.scss";

export default class EditContainer extends Component {
  constructor(props) {
    super(props);
    this.saveValue = this.saveValue.bind(this);
    this.cancel = this.cancel.bind(this);
  }
  cancel() {
    this.props.toogleEdit();
  }
  saveValue(value) {
    this.props.save(this.props.colName, this.props.row, value);
    this.props.toogleEdit();
  }
  render() {
    if (!this.props.editing) {
      return null;
    } else {
      const { children } = this.props;
      const newProps = {};
      Object.keys(this.props).forEach(prop => {
        if (prop != "children") {
          newProps[prop] = this.props[prop];
        }
      });
      newProps.saveValue = this.saveValue;
      newProps.cancel = this.cancel;
      var childrenWithProps = React.Children.map(children, child =>
        React.cloneElement(child, newProps)
      );
      let cellClasses = `le-cell-tools ${
        this.props.classes ? this.props.classes : ""
      }`;
      return( 
      <div>
        {childrenWithProps}
        
      </div>);
    }
  }
}
