import React, { Component } from "../../../../common/react-vendor";
import Aux from "../../../../common/widgets/hoc/_Aux";
import { getAngularState } from "../react/states";
import { getData } from "../../../../common/widgets/table/table-utils";

import LeGridList from "../../../../common/widgets/table/table";
import LeGridRow from "../../../../common/widgets/table/table-row";
import LeGridCell from "../../../../common/widgets/table/table-cell";
import CellContent from "../../../../common/widgets/table/cell-content";
import CellTools from "../../../../common/widgets/table/cell-tools";
import LeTableHeader from "../../../../common/widgets/table/table-header";
import LeTableBody from "../../../../common/widgets/table/table-body";


import TemplatesRowActions, {
  CREATE_TEMPLATE,
  EDIT_TEMPLATE,
  IMPORT_DATA
} from "./templates-row-actions";
import "./templates.scss";

export default class GridContainer extends Component {
  constructor(props) {
    super(props);
    this.actionCallbackHandler = this.actionCallbackHandler.bind(this);
    this.state = {
      forceReload: false,
      showEmpty: false,
      showLoading: false,
      data: []
    };
  }

  createTemplate(response) {
    let entity = "";
    switch (response.type) {
      case "Account": {
        entity = "accounts";
        break;
      }
      case "Contacts": {
        entity = "contacts";
        break;
      }
      case "Products Purchases": {
        entity = "product_purchases";
        break;
      }
      case "Product Bundles": {
        entity = "product_bundles";
        break;
      }
      case "Product Hierarchy": {
        entity = "product_hierarchy";
        break;
      }
    }
    let goTo = `home.import.entry.${entity}`;
    getAngularState().go(goTo);
  }

  actionCallbackHandler(response) {
    switch (response.action) {
      case CREATE_TEMPLATE:
        this.createTemplate(response);
        break;

      case EDIT_TEMPLATE:
        break;
      case IMPORT_DATA:
        break;
    }
  }
  getHeader() {}
  getRows() {
    if (this.state.data.length > 0) {
      let rowsUI = this.state.data.map((row, index) => {
        return (
          <LeGridRow index={index} rowData={row}>
            <LeGridCell colName="name" colSpan="2">
              <CellContent name="name">
                <span>{row.name}</span>
              </CellContent>
            </LeGridCell>

            <LeGridCell colName="object" colSpan="2">
              <CellContent name="object">
                <span>{row.object}</span>
              </CellContent>
            </LeGridCell>

            <LeGridCell colName="location" colSpan="4">
              <CellContent name="location">
                <span>{row.location}</span>
              </CellContent>
              <CellTools>
                <li
                  className="le-table-cell-icon le-table-cell-icon-actions initially-hidden"
                  title="Copy Link"
                >
                  <i className="fa fa-files-o" />
                </li>
              </CellTools>
            </LeGridCell>

            <LeGridCell colName="edited" colSpan="1">
              <CellContent name="edited">
                <span>{row.edited}</span>
              </CellContent>
            </LeGridCell>

            <LeGridCell colName="actions" colSpan="3">
              <CellTools classes="templates-controlls">
              <TemplatesRowActions
                rowData={row}
                callback={this.actionCallbackHandler}
              />
              </CellTools>
            </LeGridCell>
          </LeGridRow>
        );
      });
      return rowsUI;
    } else {
      return null;
    }
  }

  componentDidMount() {
    this.setState({
      forceReload: true,
      showEmpty: false,
      showLoading: true
    });

    setTimeout(() => {
      this.setState({
        forceReload: false,
        showEmpty: false,
        showLoading: false,
        data: getData("--")
      });
    }, 2000);
  }
  render() {
    return (
      <Aux>
        <LeGridList
          name="import-templates"
          showLoading={this.state.showLoading}
          showEmpty={this.state.showEmpty}
          emptymsg={"There is no data"}
        >
          <LeTableHeader>
            <LeGridCell colName="name" colSpan="2">
              <CellContent name="name">
                <span>Name</span>
              </CellContent>
            </LeGridCell>

            <LeGridCell colName="object" colSpan="2">
              <CellContent name="object">
                <span>Object</span>
              </CellContent>
            </LeGridCell>

            <LeGridCell colName="location" colSpan="4">
              <CellContent name="location">
                <span>Automated Import Location</span>
              </CellContent>
            </LeGridCell>

            <LeGridCell colName="edited" colSpan="1">
              <CellContent name="edited">
                <span>Last Edited</span>
              </CellContent>
            </LeGridCell>

            <LeGridCell colName="actions" colSpan="3" />
          </LeTableHeader>

          <LeTableBody>{this.getRows()}</LeTableBody>
        </LeGridList>
      </Aux>
    );
  }
}
