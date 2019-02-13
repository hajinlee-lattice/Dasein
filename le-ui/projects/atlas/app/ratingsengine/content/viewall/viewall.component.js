import React, {
  Component,
  react2angular
} from "common/react-vendor";
import LeTable from "common/widgets/table/table";
import LeLink from "common/widgets/link/le-link";
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

        let pageTitle = ""
        let data = []
        let tableConfig = {}
        switch (this.props.$state.params.type) {
          case "iterations":
            pageTitle = "Creation History";
            data = this.props.RatingsEngineStore.getIterations();
            tableConfig = {
                name: "creation-history",
                // sorting:{
                //     initial: 'none',
                //     direction: 'none'
                // },
                pagination:{
                    perPage: 10,
                    startPage: 1
                },
                header: [
                  {
                      name: "iteration",
                      displayName: "Iteration",
                      sortable: false
                  },
                  {
                      name: "status",
                      displayName: "",
                      sortable: false
                  },
                  {
                      name: "creationStatus",
                      displayName: "Creation Status",
                      sortable: false
                  },
                  {
                      name: "viewModel",
                      displayName: "",
                      sortable: false
                  }
                ],
                columns: [
                  {
                      onlyTemplate: true,
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
                      // onlyTemplate: true,
                      colSpan: 1,
                      // template: cell => {
                      //   let isActive = true;
                      //   if (isActive) {
                      //       return (
                      //           <p className="green-text">
                      //               Scoring Active
                      //           </p>
                      //       )
                      //   } else {
                      //       return (
                      //           <a>
                      //               Activate
                      //           </a>
                      //       )
                      //   }
                      // }
                  },
                  {
                    onlyTemplate: true,
                    colSpan: 8,
                    template: cell => {
                        if (cell.props.rowData.modelingJobStatus == 'Pending' || cell.props.rowData.modelingJobStatus == 'Running') {
                            return (
                                <span>
                                    <span></span>
                                    <span>{cell.props.rowData.modelingJobStatus}</span>
                                </span>
                            )
                        } else {
                            return (
                                <span>
                                    <span>{cell.props.rowData.modelingJobStatus}</span>
                                </span>
                            )
                        }
                    }
                  },
                  {
                      // onlyTemplate: true,
                      colSpan: 2,
                      // template: cell => {
                      //     return (
                      //       <button className="button link-button">View Model</button>
                      //     )
                      // }
                  }
                ]
            };
            break;

          case "usedby":
            pageTitle = "Used By";
            data = this.props.RatingsEngineStore.getUsedBy();
            tableConfig = {
                name: "used-by",
                pagination: {
                    perPage: 10,
                    startPage: 1
                },
                header: [
                    {
                        name: "type",
                        displayName: "Type",
                        sortable: true
                    },
                    {
                        name: "name",
                        displayName: "Name",
                        sortable: true
                    }
                ],
                columns: [
                    {
                        onlyTemplate: true,
                        colSpan: 2,
                        template: cell => {
                            if (cell.props.rowData) {
                                let type = cell.props.rowData.type;
                                let iconClass = '';
                                switch (type) {
                                    case "Segment": 
                                        iconClass = 'ico-segment';
                                        break;
                                    case "Campaign": 
                                        iconClass = 'ico-campaign'; 
                                        break;
                                    case "Model": 
                                        iconClass = 'ico-model';
                                        break;
                                }
                                return (
                                    <span>
                                        <span className={"icon " + iconClass}><span></span></span>
                                        {cell.props.rowData.type}
                                    </span>
                                );
                            } else {
                                return null;
                            }
                        }
                    },
                    {
                        onlyTemplate: true,
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
                ]
            };
            break;
        }

        this.state = {
            pageTitle: pageTitle,
            data: data,
            tableConfig: tableConfig
        }
    }

    backToDashboard() {
        console.log(this.props);
    }

    // <LeToolBar>
    //     <div className="left">
    //         Sort
    //         Search
    //     </div>
    // </LeToolBar>

    render() {
        return (
            <div className="view-all">
                <div className="header">
                    <LeButton
                        name="back"
                        callback={this.backToDashboard}
                        config={{
                            label: "Back",
                            classNames: "back-link"
                        }}
                    /> 
                    | {this.state.pageTitle} ({this.state.data.length})
                </div>
                <LeTable
                    name={this.state.tableConfig.name}
                    config={this.state.tableConfig}    
                    showLoading={false}
                    showEmpty={false}
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
