import React from "../../../../../common/react-vendor";
const EditControl = (props) => {


    return(
        <li
        className="le-table-cell-icon le-table-cell-icon-actions initially-hidden"
        title={props.title ? props.title : 'Edit'}
        onClick={() => {
            props.toogleEdit();
        }}
      >
        <i className={props.icon ? props.icon : "fa fa-pencil-square-o"} />
      </li>
    );
};

export default EditControl;