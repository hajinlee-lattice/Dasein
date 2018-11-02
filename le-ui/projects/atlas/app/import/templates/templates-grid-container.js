import React, { Component } from "../../../../common/react-vendor";
import { getAngularState } from "../react/states";

import httpService from "../../../../common/app/http/http-service";
import { SUCCESS } from "../../../../common/app/http/response";

import TemplatesRowActions, {
  CREATE_TEMPLATE,
  EDIT_TEMPLATE,
  IMPORT_DATA
} from "./templates-row-actions";
import "./templates.scss";
import Observer from "../../../../common/app/http/observer";
import EditControl from "../../../../common/widgets/table/controlls/edit-controls";
import EditorText from "../../../../common/widgets/table/editors/editor-text";

import messageService from "../../../../common/app/utilities/messaging-service";
import Message, {
  NOTIFICATION
} from "../../../../common/app/utilities/message";
import CopyComponent from "../../../../common/widgets/table/controlls/copy-controll";
import LeTable from "../../../../common/widgets/table/table";
export default class GridContainer extends Component {
  constructor(props) {
    super(props);
    this.actionCallbackHandler = this.actionCallbackHandler.bind(this);
    this.saveTemplateNameHandler = this.saveTemplateNameHandler.bind(this);
    this.state = {
      forceReload: false,
      showEmpty: false,
      showLoading: false,
      data: []
    };
  }

  createTemplate(response) {
    console.log(response);

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
      case "Product Purchases": {
        entity = "productpurchases";
        break;
      }
      case "Product Bundles": {
        entity = "productbundles";
        break;
      }
      case "Product Hierarchy": {
        entity = "producthierarchy";
        break;
      }
    }
    let goTo = `home.import.entry.${entity}`;

    console.log(goTo, response);

    getAngularState().go(goTo, response);
  }

  actionCallbackHandler(response) {
    switch (response.action) {
      case CREATE_TEMPLATE:
        this.createTemplate(response);
        break;
      case EDIT_TEMPLATE:
        this.createTemplate(response);
        break;
      case IMPORT_DATA:
        this.createTemplate(response);
        break;
    }
  }

  componentWillUnmount() {
    httpService.unsubscribeObservable(this.observer);
  }

  componentDidMount() {
    this.setState({
      forceReload: true,
      showEmpty: false,
      showLoading: true
    });
    this.observer = new Observer(
      response => {
        if (response.status == SUCCESS) {
          this.setState({
            forceReload: false,
            showEmpty: response.data && response.data.length == 0,
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
      },
      error => {
        this.setState({
          forceReload: false,
          showEmpty: true,
          showLoading: false,
          data: []
        });
      }
    );
    httpService.get("/pls/cdl/s3import/template", this.observer);
  }

  saveTemplateNameHandler(cell, value) {
    if (value && value != "") {
      cell.setSavingState();
      let copy = Object.assign({}, this.state.data[cell.props.row]);
      copy[cell.props.colName] = value;
      httpService.put(
        "/pls/cdl/s3/template/displayname",
        copy,
        new Observer(
          response => {
            cell.toogleEdit();
            if (response.getStatus() === SUCCESS) {
              let newState = [...this.state.data];
              newState[cell.props.row][cell.props.colName] = value;
              this.setState({ data: newState });
            }
          },
          error => {
            cell.toogleEdit();
          }
        )
      );
    }
  }
  getConfig() {
    let config = {
      name: "import-templates",
      columns: [
        {
          name: "TemplateName",
          displayName: "Name",
          colSpan: 2,
          template: cell => {
            if (!cell.state.saving && !cell.state.editing) {
              if (cell.props.rowData.Exist) {
                return (
                  <EditControl
                    icon="fa fa-pencil-square-o"
                    title="Edit Name"
                    toogleEdit={cell.toogleEdit}
                    classes="initially-hidden"
                  />
                );
              } else {
                return null;
              }
            }
            if (cell.state.editing && !cell.state.saving) {
              if (cell.props.rowData.Exist) {
                return (
                  <EditorText
                    initialValue={cell.props.rowData.TemplateName}
                    cell={cell}
                    applyChanges={this.saveTemplateNameHandler}
                    cancel={cell.cancelHandler}
                  />
                );
              } else {
                return null;
              }
            }
          },
          datasource: "/pls/cdl/s3/template/displayname"
        },
        {
          name: "Object",
          displayName: "Object",
          colSpan: 2
        },
        {
          name: "Path",
          displayName: "Automated Import Location",
          colSpan: 4,
          template: cell => {
            if (cell.props.rowData.Exist) {
              return (
                <CopyComponent
                  title="Copy Link"
                  data={cell.props.rowData[cell.props.colName]}
                  callback={() => {
                    messageService.sendMessage(
                      new Message(
                        null,
                        NOTIFICATION,
                        "success",
                        "",
                        "Copied to Clipboard"
                      )
                    );
                  }}
                />
              );
            } else {
              return null;
            }
          }
        },
        {
          name: "Edited",
          displayName: "Edited",
          colSpan: 1
        },
        {
          name: "actions",
          colSpan: 3,
          template: cell => {
            return (
              <TemplatesRowActions
                rowData={cell.props.rowData}
                callback={this.actionCallbackHandler}
              />
            );
          }
        }
      ]
    };

    return config;
  }

  render() {
    return (
      <LeTable
        name="import-templates"
        config={this.getConfig()}
        showLoading={this.state.showLoading}
        showEmpty={this.state.showEmpty}
        data={this.state.data}
      />
    );
  }
}
