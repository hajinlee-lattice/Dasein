import React, { Component } from "../../../../common/react-vendor";
import Aux from "../../../../common/widgets/hoc/_Aux";
import { getAngularState } from "../react/states";

import LeGridList from "../../../../common/widgets/table/table";
import LeGridRow from "../../../../common/widgets/table/table-row";
import LeGridCell from "../../../../common/widgets/table/table-cell";
import CellContent from "../../../../common/widgets/table/cell-content";
import CellTools from "../../../../common/widgets/table/cell-tools";
import LeTableHeader from "../../../../common/widgets/table/table-header";
import LeTableBody from "../../../../common/widgets/table/table-body";

import TemplateService from "./templates.service";
import httpService from "../../../../common/app/http/http-service";
import {SUCCESS} from '../../../../common/app/http/response';

import TemplatesRowActions, {
  CREATE_TEMPLATE,
  EDIT_TEMPLATE,
  IMPORT_DATA
} from "./templates-row-actions";
import "./templates.scss";
import Observer from "../../../../common/app/http/observer";

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
      case "Accounts": {
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
  copyPath(text) {
    // var text = "Example text to appear on clipboard";
    window.navigator.clipboard.writeText(text).then(
      () => {
        console.log("Async: Copying to clipboard was successful!");
      },
      (err) => {
        console.error("Async: Could not copy text: ", err);
      }
    );
  }
  getCopyPathUI(rowData) {
    // if (rowData.Path == "N/A") {
    //   return null;
    // } else {
    return (
      <li
        className="le-table-cell-icon le-table-cell-icon-actions initially-hidden"
        title="Copy Link"
        onClick={() => {
          this.copyPath(rowData.Path);
        }}
      >
        <i className="fa fa-files-o" />
      </li>
    );
    // }
  }

  getRows() {
    if (this.state.data.length > 0) {
      let rowsUI = this.state.data.map((row, index) => {
        return (
          <LeGridRow index={index} rowData={row}>
            <LeGridCell colName="TemplateName" colSpan="2">
              <CellContent name="TemplateName">
                <span>{row.TemplateName}</span>
              </CellContent>
            </LeGridCell>

            <LeGridCell colName="Object" colSpan="2">
              <CellContent name="Object">
                <span>{row.Object}</span>
              </CellContent>
            </LeGridCell>

            <LeGridCell colName="Path" colSpan="4">
              <CellContent name="Path">
                <span>{row.Path}</span>
              </CellContent>
              <CellTools>{this.getCopyPathUI(row)}</CellTools>
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

  componentWillUnmount(){
    httpService.unsubscribeObservable(this.observer);
  }

  componentDidMount() {
    this.setState({
      forceReload: true,
      showEmpty: false,
      showLoading: true
    });
    this.observer = new Observer((response) =>{
      if(response.status == SUCCESS){
        this.setState({
          forceReload: false,
          showEmpty: (response.data && response.data.length == 0),
          showLoading: false,
          data: response.data
        });
      } else {
        this.setState({
          forceReload: false,
          showEmpty: true,
          showLoading: false,
          data: []
        });
      }
    }, (error) => {
      this.setState({
        forceReload: false,
        showEmpty: true,
        showLoading: false,
        data: []
      });
    });
    httpService.get('/pls/cdl/s3import/template', this.observer);
    // TemplateService().getTemplates(this.observer);
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
            <LeGridCell colName="TemplateName" colSpan="2">
              <CellContent name="TemplateName">
                <span>Name</span>
              </CellContent>
            </LeGridCell>

            <LeGridCell colName="Object" colSpan="2">
              <CellContent name="Object">
                <span>Object</span>
              </CellContent>
            </LeGridCell>

            <LeGridCell colName="Path" colSpan="4">
              <CellContent name="Path">
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
