import React, { Component } from "../../react-vendor";
import PropTypes from "prop-types";
import "./table.scss";
import LeGridRow from "./table-row";
import { DISCENDENT, ASCENDENT, RELOAD } from "./table-utils";

export default class LeGridList extends Component {
  constructor(props) {
    super(props);
    console.log(this.props);
    this.state = {
      loading: false,
      data: this.props.data,
      sortBy: this.props.config.sortBy.colName,
      direction: this.props.config.sortBy.direction
    };
    this.sortByColumn = this.sortByColumn.bind(this);
  }

  getLoading() {
    // console.log("getLoading =>  ", this.state);
    if (this.state.loading == true && this.state.data.length == 0) {
      return (
        <div className="le-table-row le-table-row-no-select">
          <div className="le-table-col-span-12 le-table-cell le-table-cell-centered">
            <i class="fa fa-spinner fa-spin fa-2x fa-fw" />
          </div>
        </div>
      );
    } else {
      return null;
    }
  }
  getEmptyMsg() {
    // console.log("getEmptyMsg =>  ", this.state);
    if (!this.state.loading && this.state.data.length == 0) {
      return (
        <div className="le-table-row le-table-row-no-select">
          <div className="le-table-col-span-12 le-table-cell le-table-cell-centered">
            <p>{this.props.config.emptymsg}</p>
          </div>
        </div>
      );
    } else {
      return null;
    }
  }
  createHeaderTools(column) {
    // console.log("createHeaderTools =>  ", this.state);
    if (column.header && column.header.sorting === true) {
      let dir = "up";
      if (
        column.name == this.state.sortBy &&
        this.state.direction == ASCENDENT
      ) {
        dir = "down";
      }
      return (
        <i
          class={`le-table-cell-icon-actions fa fa-caret-${dir}`}
          aria-hidden="true"
          onClick={() => {
            this.sortByColumn(column.name);
          }}
        />
      );
    }
  }
  createHeader() {
    // console.log("createHeader =>  ", this.state);
    // console.log("Creating columns", this.props.config.columns);
    let columnsUI = this.props.config.columns.map(column => {
      let span = column.numSpan;
      let classes = `le-table-cell le-table-col-span-${span} ${
        column.class ? column.class : ""
      }`;
      return (
        <div className={classes} name={column.name}>
          {column.title}
          {this.createHeaderTools(column)}
        </div>
      );
    });
    return columnsUI;
  }
  load() {
    let tmp = this.props.data;
    this.setState({ loading: true, data: [] });
    setTimeout(() => {
      this.setState({ loading: false, data: this.props.data });
    }, 4000);
  }
  componentDidUpdate(props) {
    // console.log("************", this.props);
    if (this.props.forceReload === true && this.state.loading == false) {
      this.load();
    }
  }
  componentDidMount() {
    this.load();
  }

  createRows() {
    // console.log("createRows =>  ", this.state);
    if (this.state.data && this.state.data.length > 0) {
      let rowsUI = this.state.data.map((row, index) => {
        return (
          <LeGridRow
            config={this.props.config.columns}
            formatter={this.props.config.formatter}
            index={index}
            rowData={row}
            rowClasses={row.rowClasses}
          />
        );
      });
      return rowsUI;
    } else {
      return null;
    }
  }

  sortByColumn(colName) {
    // alert("Sort");
    if (this.state.loading == true || this.state.data.length == 0) {
      return;
    }
    let tmp = Object.assign(this.state.data);
    if (
      this.props.config.sortBy &&
      this.props.config.sortBy.clientSide === true
    ) {
      console.log("NAME ", colName);
      let direction = DISCENDENT;
      if (this.state.direction === DISCENDENT) {
        direction = ASCENDENT;
      } else {
        direction = DISCENDENT;
      }
      this.setState({
        loading: true,
        data: [],
        sortBy: colName,
        direction: direction
      });
      setTimeout(() => {
        this.setState({
          loading: false,
          data: tmp
        });
      }, 4000);
    }
  }

  

  render() {
    // console.log("RENDERING ", this.state);
    return (
      <div className={`le-table ${this.props.name}`}>
        <div className="le-table-header le-table-row-no-select">
          {this.createHeader()}
        </div>
        {this.createRows()}
        {this.getEmptyMsg()}
        {this.getLoading()}
      </div>
    );
  }
}

LeGridList.propTypes = {
  name: PropTypes.name,
  config: PropTypes.object,
  data: PropTypes.array
};
