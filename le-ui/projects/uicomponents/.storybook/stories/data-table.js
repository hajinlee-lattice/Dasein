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
import LeButton from '../../../common/widgets/buttons/le-button';
import { getData } from "../../../common/widgets/table/table-utils";
const stories = storiesOf("Data Table", module);

stories.addDecorator(withKnobs);
let data = getData("--");

let config = {
  name: "import-templates",
  header: [
    {
      name: "TemplateName",
      displayName: "Name",
      sortable: false
    },
    {
      name: "Object",
      displayName: "Object",
      sortable: false
    },
    {
      name: "Path",
      displayName: "Automated Import Location",
      sortable: false
    },
    {
      name: "LastEditedDate",
      displayName: "Edited",
      sortable: false
    },
    {
      name: "actions",
      sortable: false
    }
  ],
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
            name="action"
            callback={action("button-click")}
            config={{
              classNames: 'orange-button',
              label: 'Action',
              icon: "fa fa-cloud-upload"
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
    config={config}
    showLoading={boolean("showLoading", false)}
    showEmpty={boolean("showEmpty", false)}
    data={data}
  />
));
