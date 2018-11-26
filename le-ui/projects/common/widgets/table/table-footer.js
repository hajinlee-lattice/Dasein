import React, { Component } from "../../react-vendor";
import propTypes from "prop-types";
import "./table.scss";

import LeTableRow from "./table-row";
import Aux from "../hoc/_Aux";
import LePagination from "../pagination/le-pagination";

export default class LeTableFooter extends Component {
  constructor(props) {
    super(props);
  }

  //   getFooter(){

  //     return(
  //     //   <div className="le-table-cell le-table-col-span-12 le-table-footer-container">

  //     //   </div>
  //     )
  //   }

  render() {
    return (
      <div className="le-table-footer">
        <div className="le-table-cell le-table-col-span-12">
          <LePagination
            classesName="table-pagination"
            total={this.props.total}
            perPage={this.props.perPage}
            start={this.props.start}
            callback={this.props.callback}
          />
        </div>
      </div>
    );
  }
}
