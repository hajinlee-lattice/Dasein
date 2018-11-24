import React, { Component } from "../../react-vendor";
import "./le-pagination.scss";

export default class LePagination extends Component {
  constructor(props) {
    super(props);
    
    this.clickHandler = this.clickHandler.bind(this);
    this.getSubset = this.getSubset.bind(this);
    this.init();
    this.state = { current: props.start };
  }

  init(){
    this._data = this.props.data;
    this._startPage = this.props.start;
    this._perPage = this.props.perPage;
    this._numPages = this._data.length / this._perPage;
    let tmp = this._data.length % this._perPage;
    if (tmp > 0) {
      this._numPages = Math.ceil(this._numPages);
    }
  }
  
  getSubset(page){
    let from = (page - 1) * this._perPage;
    let to = page * this._perPage;
    if (to <= this._data.length) {
      return this._data.slice(from, to);
    } else {
      return this._data.slice(from, this._data.length);
    }
  }

  getFormTo(page){
    let from = (page - 1) * this._perPage;
    let to = page * this._perPage;
    if(to > this._data.length){
        to = this._data.length;
    }
    return {from : from, to: to};
  }

  componentDidMount(){
    this.clickHandler('first');
  }

  clickHandler(direction) {
    switch (direction) {
        case "first": 
        this.setState({ current: 1 }, () => {
            this.props.callback(this.getSubset(this.state.current), this.getFormTo(this.state.current));
          });
        break;
      case "next":
        if (this.state.current < this._numPages) {
          this.setState({ current: this.state.current + 1 }, () => {
            this.props.callback(this.getSubset(this.state.current), this.getFormTo(this.state.current));
          });
        }
        break;

      case "prev":
        if (this.state.current > 1) {
          this.setState({ current: this.state.current - 1 }, () => {
            this.props.callback(this.getSubset(this.state.current), this.getFormTo(this.state.current));
          });
        }

        break;
        case 'last':
        this.setState({ current: this._numPages }, () => {
            this.props.callback(this.getSubset(this.state.current), this.getFormTo(this.state.current));
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
          <span className="pd-pagination-pagetotal">{this._numPages}</span>
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
