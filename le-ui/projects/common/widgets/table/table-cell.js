import React, { Component } from "../../react-vendor";
import PropTypes from "prop-types";
import "./table.scss";

export default class LeGridCell extends Component {
  constructor(props) {
    super(props);
    this.state = { editing: false, saving: false };
    this.editable = this.props.editable ? this.props.editable : false;
    this.toogleEdit = this.toogleEdit.bind(this);
    this.saveHandler = this.saveHandler.bind(this);
  }

  saveHandler(value) {
    this.setState({saving : true}, () =>{
      this.props.apply(this.props.colName, this.props.row, value, () => {
        this.toogleEdit();
      });
    });
    
  }

  getSaving(){
    if(this.state.saving){
      return <div><i class="fa fa-spinner fa-spin fa-fw" /></div>;
    }else{
      return null;
    }
  }

  
  update(){
    this.setState({ editing: !this.state.editing });
  }
  toogleSaving(){
    this.setState({saving: !this.state.saving});
  }
  toogleEdit() {
    if (this.editable) {
      // this.update();
      this.setState({ editing: !this.state.editing, saving: false });

    }
  }

  render() {
    const { children } = this.props;
    const newProps = {};
    Object.keys(this.props).forEach(prop => {
      if(prop != 'children'){
        newProps[prop] = this.props[prop];
      }
    });
    newProps.cancel = this.toogleEdit;
    newProps.toogleEdit = this.toogleEdit;
    newProps.editing = this.state.editing;
    newProps.saving = this.state.saving;
    newProps.applyChanges = this.saveHandler;
    var childrenWithProps = React.Children.map(children, child =>
      React.cloneElement(child, newProps)
    );

    let span = `le-table-cell le-table-col-span-${this.props.colSpan} cell-${
      this.props.row
    }-${this.props.col} ${this.props.colName}`;
    let externalFormatting = "";
    if (this.props.config && this.props.config.formatter) {
      externalFormatting = this.props.config.formatter(this.props.rowData);
    }
    let format = `${span} ${externalFormatting}`;
    return <ul className={format}>
    <div>{childrenWithProps}</div>
      {this.getSaving()}
    </ul>;
  }
}

LeGridCell.propTypes = {};
