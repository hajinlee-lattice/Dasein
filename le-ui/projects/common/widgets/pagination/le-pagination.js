import React, { Component } from "../../react-vendor";
import "./le-pagination.scss";

let _data = [];
let _startPage = 0;
let _perPage = 10;
let _numPages = 1;
export const PaginationUtil = {
  init: (dataArray, startPage, perPage) => {
    _data = dataArray;
    _startPage = startPage;
    _perPage = perPage;
    _numPages = dataArray.length / perPage;
    let tmp = dataArray.length % perPage;
    if (tmp > 0) {
      _numPages = Math.ceil(_numPages);
    }
  },
  getNumPages: () => {
    return _numPages;
  },
  getStartPage : () => {
      return _startPage;
  },
  getSubset: page => {
    let from = (page - 1) * _perPage;
    let to = page * _perPage;
    if (to <= _data.length) {
      return _data.slice(from, to);
    } else {
      return _data.slice(from, _data.length);
    }
  }
};
export default class LePagination extends Component {
  constructor(props) {
    super(props);
    
    this.clickHandler = this.clickHandler.bind(this);
    PaginationUtil.init(this.props.data, this.props.start, this.props.perPage);
    this.state = { pages: PaginationUtil.getNumPages(), current: props.start };
  }
  componentDidMount(){
    
    console.log(PaginationUtil.getNumPages());
    
    
    this.clickHandler('first');
  }

  clickHandler(direction) {
    switch (direction) {
        case "first": 
        this.setState({ current: 1 }, () => {
            this.props.callback(PaginationUtil.getSubset(this.state.current));
          });
        break;
      case "next":
        if (this.state.current < this.state.pages) {
          this.setState({ current: this.state.current + 1 }, () => {
            this.props.callback(PaginationUtil.getSubset(this.state.current));
          });
        }
        break;

      case "prev":
        if (this.state.current > 1) {
          this.setState({ current: this.state.current - 1 }, () => {
            this.props.callback(PaginationUtil.getSubset(this.state.current));
          });
        }

        break;
        case 'last':
        this.setState({ current: this.state.pages }, () => {
            this.props.callback(PaginationUtil.getSubset(this.state.current));
          });
        break;
    }
  }
  render() {
    return (
      <div
        className={`pd-pagination ${
          this.props.classesName ? this.props.classesName : ""
        }`}
      >
        <button
          type="button"
          className="button borderless-button pd-pagination-start"
          onClick={() => {
            this.clickHandler("first");
          }}
        >
          <span className="fa fa-angle-double-left" />
        </button>
        <button
          type="button"
          className="button borderless-button pd-pagination-prev"
          onClick={() => {
            this.clickHandler("prev");
          }}
        >
          <span className="fa fa-angle-left" />
        </button>

        <span className="pd-pagination-center">
          <span className="pd-pagination-pagenum">{this.state.current}</span>
          <span>/</span>
          <span className="pd-pagination-pagetotal">{this.state.pages}</span>
        </span>

        <button
          type="button"
          className="button borderless-button pd-pagination-next"
          onClick={() => {
            this.clickHandler("next");
          }}
        >
          <span className="fa fa-angle-right" />
        </button>
        <button
          type="button"
          className="button borderless-button pd-pagination-end"
          onClick={() => {
            this.clickHandler("last");
          }}
        >
          <span className="fa fa-angle-double-right" />
        </button>
      </div>
    );
  }
}
