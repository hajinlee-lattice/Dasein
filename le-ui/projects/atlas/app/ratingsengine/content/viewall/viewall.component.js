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
                    direction: 'desc'
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

                        let isActive = false;
                        let iterationId = cell.props.rowData.id;
                        let iterationCompleted = cell.props.rowData.modelingJobStatus == 'Completed';
                        let ratingEngine = this.props.RatingsEngineStore.getRatingEngine();

                        if (ratingEngine.scoring_iteration != null) {
                            if (ratingEngine.scoring_iteration.AI.id == iterationId) {
                                isActive = true;
                            } else {
                                isActive = false;
                            }
                        } else {
                            isActive = false;
                        }

                        if (isActive && ratingEngine.status == 'ACTIVE') {
                            return (
                              <span className="active">{cell.props.rowData.iteration}</span>
                            )
                        } else {
                          return (
                            <span>{cell.props.rowData.iteration}</span>
                          )
                        }
                      }
                  },
                  {
                      onlyTemplate: true,
                      colSpan: 2,
                      template: cell => {

                        let isScoringActive = false;
                        let showActivateButton = false;
                        let iterationId = cell.props.rowData.id;
                        let iterationCompleted = cell.props.rowData.modelingJobStatus == 'Completed';
                        let ratingEngine = this.props.RatingsEngineStore.getRatingEngine();

                        if (ratingEngine.scoring_iteration != null) {
                            if (ratingEngine.scoring_iteration.AI.id == iterationId) {
                                isScoringActive = true;
                            } else {
                                isScoringActive = false;
                            }
                        } else {
                            isScoringActive = false;
                        }

                        if (ratingEngine.status == 'ACTIVE') {
                            if (isScoringActive) {
                                showActivateButton = false;
                            } else {
                                showActivateButton = iterationCompleted ? true : false;
                            }
                        } else {
                            showActivateButton = iterationCompleted ? true : false; 
                        }

                        if (showActivateButton) {

                          return (
                              <LeButton
                                  name="activate"
                                  callback={() => {
                                      this.props.$state.go('home.model.ratings', { 
                                        rating_id: this.props.$stateParams.rating_id, 
                                        modelId: cell.props.rowData.modelSummaryId, 
                                        section: 'dashboard.ratings', 
                                        ratingEngine: ratingEngine,
                                        useSelectedIteration: true
                                      })
                                  }}
                                  config={{
                                      label: "Activate",
                                      classNames: "activate-button"
                                  }}
                                />
                          )
                        } else if (!showActivateButton && isScoringActive && iterationCompleted) {
                          return (
                              <p className="green-text">
                                  Scoring Active
                              </p>
                          )
                        } else {
                          return '';
                        }
                      }
                  },
                  {
                    onlyTemplate: true,
                    colSpan: 7,
                    template: cell => {

                        let pending = cell.props.rowData.modelingJobStatus == 'Pending';
                        let running = cell.props.rowData.modelingJobStatus == 'Running';
                        let completed = cell.props.rowData.modelingJobStatus == 'Completed';
                        let cancelled = cell.props.rowData.modelingJobStatus == 'Cancelled';

                        let timestamp = new Date(cell.props.rowData.created);
                        const formattedDate = timestamp.toDateString().split(' ').slice(1).join(' ');

                        if (pending || running) {
                            return (
                                <span className="running">
                                    <span></span>
                                    <span>Creating</span>
                                </span>
                            )
                        } else if (completed) {
                            return (
                                <span className="completed">
                                    <span>Successfully created {formattedDate}</span>
                                </span>
                            )
                        } else if (cancelled) {
                          return (
                                <span className="failed">
                                    <span>Cancelled {formattedDate}</span>
                                </span>
                            )
                        } else {
                            return (
                                <span className="failed">
                                    <span>Failed {formattedDate}</span>
                                </span>
                            )
                        }
                    }
                  },
                  {
                      onlyTemplate: true,
                      colSpan: 2,
                      template: cell => {

                          let completed = cell.props.rowData.modelingJobStatus == 'Completed';

                          if (completed) {
                            return (
                                <LeButton
                                  name="view-model"
                                  callback={() => {
                                      this.props.RatingsEngineStore.setRemodelIteration(cell.props.rowData);
                                      this.props.$state.go('home.model.datacloud', { rating_id: this.props.$stateParams.rating_id, modelId: cell.props.rowData.modelSummaryId, aiModel: cell.props.rowData.id})
                                  }}
                                  config={{
                                      label: "View Model",
                                      classNames: "link-button"
                                  }}
                                />
                            )  
                          } else {
                            return '';
                          }
                          
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
                        callback={() => {
                            this.props.$state.go('home.ratingsengine.dashboard', { rating_id: this.props.$stateParams.rating_id, modelId: this.props.$stateParams.modelId})
                        }}
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
