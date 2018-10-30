import React, { Component } from "../../react-vendor";
import PropTypes from "prop-types";
import "./table.scss";

export default class EditContainer extends Component {
  constructor(props) {
    super(props);
  }

  render() {
    if (!this.props.editing || this.props.saving) {
      return null;
    } else {
      const { children } = this.props;
      const newProps = {};
      Object.keys(this.props).forEach(prop => {
        if (prop != "children") {
          newProps[prop] = this.props[prop];
        }
      });

      var childrenWithProps = React.Children.map(children, child =>
        React.cloneElement(child, newProps)
      );
      let cellClasses = `${this.props.classes ? this.props.classes : ""}`;
      return <div className={cellClasses}>{childrenWithProps}</div>;
    }
  }
}
