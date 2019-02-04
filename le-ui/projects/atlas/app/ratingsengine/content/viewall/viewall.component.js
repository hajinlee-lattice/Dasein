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
    let tableColumns = []
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
          }
        ];
        tableColumns = [
          {
              colSpan: 1,
              template: cell => {
                if (cell.props.rowData) {
                  return (
                    cell.props.rowData.iteration
                  )
                } else {
                  return null;
                }
              }
          },
          {
              colSpan: 1,
              template: cell => {
                if (cell.props.rowData) {
                  return (
                    ''
                  )
                } else {
                  return null;
                }
              }
          },
          {
              colSpan: 10,
              template: cell => {
                if (cell.props.rowData) {
                  return (
                    cell.props.rowData.modelingJobStatus
                  )
                } else {
                  return null;
                }
              }
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
        tableColumns = [
          {
              colSpan: 2,
              template: cell => {
                if (cell.props.rowData) {
                  return (
                    cell.props.rowData.type
                  )
                } else {
                  return null;
                }
              }
          },
          {
              colSpan: 10,
              template: cell => {
                if (cell.props.rowData) {
                  return (
                    cell.props.rowData.name
                  )
                } else {
                  return null;
                }
              }
          }
        ];
        break;
    }

    this.state = {
        forceReload: false,
        showEmpty: false,
        showLoading: false,
        data: data,
        tableHeader: tableHeader,
        tableColumns: tableColumns
    };
  }

  getConfig() {
      let config = {
          name: "viewall",
          header: this.state.tableHeader,
          columns: this.state.tableColumns
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
