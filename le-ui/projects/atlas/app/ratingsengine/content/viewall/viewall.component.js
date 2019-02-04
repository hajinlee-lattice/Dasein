import React, {
  Component,
  react2angular
} from "common/react-vendor";
import LeTable from "common/widgets/table/table";
import LeButton from "common/widgets/buttons/le-button";
import TemplatesRowActions, {
    CREATE_TEMPLATE
} from "./viewall-row-actions";
import {
  LeToolBar,
  VERTICAL
} from "common/widgets/toolbar/le-toolbar";
import "./viewall.scss";

class ViewAllComponent extends Component {
  constructor(props) {
    super(props);

    let data = []
    let tableHeader = []
    switch (this.props.$state.params.type) {
      case "iterations":
        data = this.props.RatingsEngineStore.getIterations();
        tableHeader = [
          {
              name: "Iteration",
              displayName: "Iteration",
              sortable: false
          },
          {
              name: "Status",
              displayName: "",
              sortable: false
          },
          {
              name: "CreationStatus",
              displayName: "Creation Status",
              sortable: false
          },
          {
              name: "Status",
              displayName: "",
              sortable: false
          }
        ];
        break;

      case "usedby":
        data = this.props.RatingsEngineStore.getUsedBy();
        tableHeader = [
          {
              name: "Type",
              displayName: "Type",
              sortable: false
          },
          {
              name: "Name",
              displayName: "Name",
              sortable: false
          }
        ];
        break;
    }

    this.state = {
        forceReload: false,
        showEmpty: false,
        showLoading: false,
        data: data,
        tableHeader: tableHeader
    };
  }

  getConfig() {
      let config = {
          name: "viewall",
          header: this.state.tableHeader,
          columns: [
              {
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
                                      initialValue={
                                          cell.props.rowData.TemplateName
                                      }
                                      cell={cell}
                                      applyChanges={
                                          this.saveTemplateNameHandler
                                      }
                                      cancel={cell.cancelHandler}
                                  />
                              );
                          } else {
                              return null;
                          }
                      }
                  }
              },
              {
                  colSpan: 2
              },
              {
                  colSpan: 3,
                  template: cell => {
                      if (cell.props.rowData.Exist) {
                          return (
                              <CopyComponent
                                  title="Copy Link"
                                  data={
                                      cell.props.rowData[cell.props.colName]
                                  }
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
                  colSpan: 2,
                  mask: value => {
                      var options = {
                          year: "numeric",
                          month: "2-digit",
                          day: "2-digit",
                          hour: "2-digit",
                          minute: "2-digit"
                      };
                      var formatted = new Date(value);
                      console.log(
                          `grid formatted: ${formatted} value: ${value} options: ${options}`
                      );
                      var buh = "err";
                      try {
                          buh = formatted.toLocaleDateString(
                              "en-US",
                              options
                          );
                      } catch (e) {
                          console.log(e);
                      }

                      return buh;
                  }
              },
              {
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
      <div>
          <LeTable
              name="viewall"
              config={this.getConfig()}
              forceReload={this.state.forceReload}
              showLoading={this.state.showLoading}
              showEmpty={this.state.showEmpty}
              data={this.state.data}
          />
      </div>
    );
  }
}

angular
  .module("le.ratingsengine.viewall", [])
  .component(
    "viewAllComponent",
    react2angular(ViewAllComponent, [], ["$state", "RatingsEngineStore"])
  );
