import React, { Component } from "../../react-vendor";
import PropTypes from "prop-types";
import "./table.scss";

export default class CellTools extends Component {
  constructor(props) {
    super(props);
    console.log('CELL TOOLS ',props);
  }

  render() {
    
    if (this.props.editing) {
      return null;
    } else {
      console.log(this.props);
      const { children } = this.props;
      const newProps = {};
      Object.keys(this.props).forEach(prop => {
        if (prop != "children") {
          newProps[prop] = this.props[prop];
        }
      });
      var childrenWithProps = React.Children.map(children, child => {
        if (child != null) {
          return React.cloneElement(child, newProps);
        }
      });
      let cellClasses = `le-cell-tools ${
        this.props.classes ? this.props.classes : ""
      }`;
      return <li className={cellClasses}>{childrenWithProps}</li>;
    }
  }
}
