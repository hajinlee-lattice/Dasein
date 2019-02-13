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
                sorting:{
                    initial: 'iteration',
                    direction: 'dis'
                },
                pagination:{
                    perPage: 10,
                    startPage: 1
                },
                header: [
                  {
                      name: "iteration",
                      displayName: "Iteration",
                      sortable: true
                  },
                  {
                      name: "status",
                      displayName: "",
                      sortable: false
                  },
                  {
                      name: "creationStatus",
                      displayName: "Creation Status",
                      sortable: true
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
                      onlyTemplate: true,
                      colSpan: 1,
                      template: cell => {
                        let isActive = true;
                        if (isActive) {
                            return (
                                <p className="green-text">
                                    Scoring Active
                                </p>
                            )
                        } else {
                            return (
                                <button>
                                    Activate
                                </button>
                            )
                        }
                      }
                  },
                  {
                    onlyTemplate: true,
                    colSpan: 8,
                    template: cell => {

                        let pending = cell.props.rowData.modelingJobStatus == 'Pending';
                        let running = cell.props.rowData.modelingJobStatus == 'Running';
                        let completed = cell.props.rowData.modelingJobStatus == 'Completed';

                        if (pending || running) {
                            return (
                                <span className="running">
                                    <span></span>
                                    <span>{cell.props.rowData.modelingJobStatus}</span>
                                </span>
                            )
                        } else {
                            return (
                                <span className={(completed ? 'completed' : 'failed')}>
                                    <span>{cell.props.rowData.modelingJobStatus}</span>
                                </span>
                            )
                        }
                    }
                  },
                  {
                      onlyTemplate: true,
                      colSpan: 2,
                      template: cell => {
                          return (
                              <LeButton
                                name="viewModel"
                                callback={() => {
                                    console.log(this.props.$stateParams);
                                    this.props.$state.go('home.model.datacloud', { rating_id: this.props.$stateParams.rating_id, modelId: cell.props.rowData.modelSummaryId, aiModel: cell.props.rowData.id})
                                }}
                                config={{
                                    label: "View Model",
                                    classNames: "back-link"
                                }}
                              />
                          )
                      }
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
            tableConfig: tableConfig,
            ratingId: '',
            modelId: '',
            aiModel: ''
        }
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
    react2angular(ViewAllComponent, [], ["$state", "$stateParams", "RatingsEngineStore"])
  );
