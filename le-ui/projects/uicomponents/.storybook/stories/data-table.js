import React from "../../../common/react-vendor";
import { storiesOf } from "@storybook/react";
import { action } from "@storybook/addon-actions";
import {
  withKnobs,
  text,
  boolean,
  select,
  number
} from "@storybook/addon-knobs";
import "../../../common/assets/css/font-awesome.min.css";
import "../../../common/widgets/layout/layout.scss";

import LeTable from "../../../common/widgets/table/table";
import CopyComponent from "../../../common/widgets/table/controlls/copy-controll";
import EditControl from "../../../common/widgets/table/controlls/edit-controls";
import EditorText from "../../../common/widgets/table/editors/editor-text";
import LeButton from '../../../common/widgets/buttons/le-button';
import { getData } from "../../../common/widgets/table/table-utils";
const stories = storiesOf("Data Table", module);

stories.addDecorator(withKnobs);
let data = getData("--");

let configSimple = {
  name: "import-templates",
  header: [
    {
      name: "TemplateName",
      displayName: "Name"
    },
    {
      name: "Object",
      displayName: "Object"
    },
    {
      name: "Path",
      displayName: "Automated Import Location"
    },
    {
      name: "LastEditedDate",
      displayName: "Edited"
    },
    {
      name: "actions"
    }
  ],
  columns: [
    {
      colSpan: 2
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
              data={cell.props.rowData[cell.props.colName]}
              callback={action("copy link")}
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
        return formatted.toLocaleDateString("en-US", options);
      }
    },
    {
      colSpan: 3,
      template: cell => {
        return (
          <LeButton
          name="borderless"
          callback={action("button-click")}
          disabled={false}
          config={{
            classNames:  "borderless-button",
            icon: text("configSimple.icon", "fa fa-cloud-upload")
          }}
        />
        );
      }
    }
  ]
};
stories.add("table", () => (
  <LeTable
    name="lattice-simple"
    config={configSimple}
    showLoading={boolean("showLoading", false)}
    showEmpty={boolean("showEmpty", false)}
    data={data}
  />
));

// Sorting table


let configSorting = {
  name: "sorting-table",
  sorting:{
    initial: 'none',
    direction: 'none'
  },
  header: [
    {
      name: "TemplateName",
      displayName: "Name",
      sortable: true
    },
    {
      name: "Object",
      displayName: "Object",
      sortable: false
    },
    {
      name: "Path",
      displayName: "Automated Import Location",
      sortable: true
    },
    {
      name: "LastEditedDate",
      displayName: "Edited",
      sortable: true
    }
  ],
  columns: [
    {
      colSpan: 3
    },
    {
      colSpan: 3
    },
    {
      colSpan: 3,
      template: cell => {
        if (cell.props.rowData.Exist) {
          return (
            <CopyComponent
              title="Copy Link"
              data={cell.props.rowData[cell.props.colName]}
              callback={action("copy link")}
            />
          );
        } else {
          return null;
        }
      }
    },
    {
      colSpan: 3,
      mask: value => {
        var options = {
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
          hour: "2-digit",
          minute: "2-digit"
        };
        var formatted = new Date(value);
        return formatted.toLocaleDateString("en-US", options);
      }
    }
  ]
};

stories.add("sorting table", () => (
  <LeTable
    name={configSorting.name}
    config={configSorting}
    showLoading={boolean("showLoading", false)}
    showEmpty={boolean("showEmpty", false)}
    data={data}
  />
));

// Edit table


let configEdit = {
  name: "sorting-table",
 
  header: [
    {
      name: "TemplateName",
      displayName: "Name"
    },
    {
      name: "Object",
      displayName: "Object"
    },
    {
      name: "Path",
      displayName: "Automated Import Location"
    },
    {
      name: "LastEditedDate",
      displayName: "Edited"
    }
  ],
  columns: [
    {
      colSpan: 3,
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
                applyChanges={(cell, value) => {
                  cell.toogleEdit();
                }}
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
      colSpan: 3
    },
    {
      colSpan: 3,
      template: cell => {
        if (cell.props.rowData.Exist) {
          return (
            <CopyComponent
              title="Copy Link"
              data={cell.props.rowData[cell.props.colName]}
              callback={action("copy link")}
            />
          );
        } else {
          return null;
        }
      }
    },
    {
      colSpan: 3,
      mask: value => {
        var options = {
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
          hour: "2-digit",
          minute: "2-digit"
        };
        var formatted = new Date(value);
        return formatted.toLocaleDateString("en-US", options);
      }
    }
  ]
};

stories.add("edit table", () => (
  <LeTable
    name={configEdit.name}
    config={configEdit}
    showLoading={boolean("showLoading", false)}
    showEmpty={boolean("showEmpty", false)}
    data={data}
  />
));
