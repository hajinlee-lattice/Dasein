import React, { Component } from "../../../common/react-vendor";
import Aux from "../../../common/widgets/hoc/_Aux";
import LeGridList from "../../../common/widgets/table/table";
import LeButton from "../../../common/widgets/buttons/le-button";
import {
  VISIBLE,
  TYPE_STRING,
  TYPE_OBJECT,
  DISCENDENT,
  getData,
  ASCENDENT
} from "../../../common/widgets/table/table-utils";
import LeLink, { RIGHT } from "../../../common/widgets/link/le-link";
import "./table-container.scss";

export default class TableContainer extends Component {
  constructor(props) {
    super(props);
    this.createTemplateHandler = this.createTemplateHandler.bind(this);
    this.state = {forceReload: false};
    this.config = {
      emptymsg: "Ther is no data",
      formatter: data => {
        if (data.object === "Contacts") {
          return "";
        }
        if (data.object === "Product Bundles") {
          return "";
        }
      },
      sortBy: {
        clientSide: true,
        colName: "name",
        direction: ASCENDENT
      },
      datasource: {
        local: false,
        url: ''
      },
      columns: [
        {
          name: "name",
          title: "Template Name",
          numSpan: 2,
          type: TYPE_STRING,
          header: {
            sorting: true
          },
          cell: {
            toolsState: VISIBLE,
            formatter: data => {
              if (data.object === "Account") {
                return "";
              }
            },
            icon: data => {
              if (data.object == "Account") {
                return <i className="fa fa-fighter-jet le-table-cell-icon" />;
              } else {
                return <i className="fa fa-thumbs-up le-table-cell-icon" />;
              }
            },
            tools: rowData => {
              // console.log('ROW DATA ', rowData);
              return (
                <li className="le-table-cell-icon le-table-cell-icon-actions" title="Edit Name">
                  <i className="fa fa-pencil-square-o" />
                </li>
              );
            }
          }
        },
        {
          name: "object",
          title: "Object",
          numSpan: 2,
          cell: {
            formatter: data => {
              if (data.object === "Account") {
                return "";
              }
            }
          }
        },
        {
          name: "location",
          title: "Automated Import Location",
          numSpan: 4,
          type: TYPE_STRING,
          cell: {
            tools: rowData => {
              return (
                <li className="le-table-cell-icon le-table-cell-icon-actions" title="Copy Link">
                  <i className="fa fa-files-o" />
                </li>
              );
            }
          }
        },
        {
          name: "edited",
          title: "Last Edited",
          numSpan: 1,
          header: {
            sorting: true
          },
          cell: {}
        },
        {
          name: "actions",
          title: "",
          numSpan: 3,
          type: TYPE_OBJECT,
          cell: {
            tools: rowData => {
              return (
                <div className="templates-main-container">
                  <LeLink
                    config={{
                      label: "Create Template",
                      classes: "always borders-over",
                      name: ""
                    }}
                    callback={() => {
                      this.createTemplateHandler(rowData.object);
                    }}
                  />
                </div>
              );
            }
          }
        }
      ]
    };
  }

  getLink() {
    return (
      <LeLink
        config={this.linkConfig}
        callback={() => {
          // this.createTemplateHandler();
        }}
      />
    );
  }

  createTemplateHandler(type) {
    let entity = "";
    switch (type) {
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
    alert('Entity ', entity)
  }

  render() {
    return (
      <Aux>
        <button
          onClick={() => {
            this.data = [];
            // this.forceUpdate();
            this.setState({forceReload: true});
            setTimeout(()=> {
              this.setState({forceReload: false});
            }, 100);
          }}
        >
          Reload
        </button>
        <LeGridList
          name="import-templates"
          forceReload={this.state.forceReload}
          data={getData('---')}
          config={this.config}
        />
      </Aux>
    );
  }
}
