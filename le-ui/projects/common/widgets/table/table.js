import React, { Component } from "common/react-vendor";
import propTypes from "prop-types";

import LeTableHeader from "./table-header";
import LeTableBody from "./table-body";
import "./table.scss";
import Sort, { DIRECTION_NONE, SortUtil } from "./controlls/sort";
import LeTableFooter from "./table-footer";

export default class LeTable extends Component {
    constructor(props) {
        super(props);
        this.state = {
            showLoading: this.props.showLoading,
            showEmpty: this.props.showEmpty,
            perPage: this.props.config.pagination
                ? this.props.config.pagination.perPage
                : 10,
            startPage: this.props.config.pagination
                ? this.props.config.pagination.startPage
                : 1,
            rowsSelected: {},
            selectable: this.props.config.selectable ? this.props.config.selectable : null
        };
        if (props.config.sorting) {
            this.state.sortingName = this.props.config.sorting
                ? this.props.config.sorting.initial
                : DIRECTION_NONE;
            this.state.sortingDirection = this.props.config.sorting
                ? this.props.config.sorting.direction
                : DIRECTION_NONE;
        }

        this.sortHandler = this.sortHandler.bind(this);
        this.setColumnSorting = this.setColumnSorting.bind(this);
        this.loadPage = this.loadPage.bind(this);
        this.clickRowHandler = this.clickRowHandler.bind(this);
        this.columnsMapping = {};
        this.headerMapping = {};
        this.props.config.header.forEach((header, index) => {
            this.headerMapping[header.name] = Object.assign({}, header);
            this.headerMapping[header.name].colSpan = this.props.config.columns[
                index
            ].colSpan;
            this.setColumnSorting(header);
            const newItem = Object.assign(
                header,
                this.props.config.columns[index]
            );
            newItem.colIndex = index;
            this.columnsMapping[header.name] = newItem;
        });
    }

    loadPage(fromToObj) {
        // this.setState({ data: data });
        if (fromToObj) {
            this.setState({
                data: this.props.data.slice(fromToObj.from, fromToObj.to)
            });
        }
    }
    sortHandler(colName, direction) {
        this.setState(
            {
                sortingName: colName,
                sortingDirection: direction,
                showLoading: true
            },
            () => {
                let newData = SortUtil.sortAray(
                    this.state.data,
                    this.state.sortingName,
                    this.state.sortingDirection
                );

                this.setColumnsSorting();
                this.setState({ data: newData, showLoading: false });
            }
        );
    }
    setColumnsSorting() {
        this.props.config.header.forEach(header => {
            this.setColumnSorting(header);
        });
    }

    clickRowHandler(rowIndex, data) {
        
        this.setState({ rowsSelected: { [rowIndex]: data } },
            () => {
                if (this.props.onClick) {
                    // this.setState({ rowsSelected: { [rowIndex]: data } });
                    this.props.onClick(this.state.rowsSelected, rowIndex);
                }
            });



    }

    setColumnSorting(header) {
        if (this.props.config.sorting && header && header.sortable) {
            this.headerMapping[header.name].template = (function (s, h, cb) {
                return function () {
                    let direction = DIRECTION_NONE;
                    if (header.name == s.sortingName) {
                        direction = s.sortingDirection;
                    }
                    return (
                        <Sort
                            direction={direction}
                            colName={h.name}
                            callback={(colName, direction) => {
                                cb(colName, direction);
                            }}
                        />
                    );
                };
            })(this.state, header, this.sortHandler);
        }
    }
    getLoading() {
        if (
            (this.props.showLoading && !this.props.showEmpty) ||
            (this.state && this.state.showLoading)
        ) {
            return (
                <div className="le-table-row-no-select le-table-col-span-12 le-table-cell le-table-cell-centered indicator-row">
                    <i className="fa fa-spinner fa-spin fa-2x fa-fw" />
                </div>
            );
        } else {
            return null;
        }
    }
    getEmptyMsg() {
        if (this.props.showEmpty && !this.props.showLoading) {
            return (
                <div className="le-table-row-no-select le-table-col-span-12 le-table-cell le-table-cell-centered indicator-row">
                    <p>
                        {this.props.emptymsg
                            ? this.props.emptymsg
                            : "No data available"}
                    </p>
                </div>
            );
        } else {
            return null;
        }
    }
    getBody() {
        if (
            this.props &&
            !this.props.showEmpty &&
            !this.props.showLoading &&
            this.state &&
            !this.state.showLoading
        ) {
            return (
                <LeTableBody
                    columnsMapping={this.columnsMapping}
                    data={this.state.data}
                    rowsSelected={this.state.rowsSelected}
                    onClick={this.state.selectable ? this.clickRowHandler : null}
                />
            );
        } else {
            return null;
        }
    }

    getFooter() {
        return (
            <LeTableFooter
                total={this.props.data.length}
                callback={this.loadPage}
                perPage={this.state.perPage}
                start={this.state.startPage}
            />
        );
    }

    static getDerivedStateFromProps(nextProps, prevState) {
        if (nextProps.forceReload) {
            return { data: nextProps.data, showLoading: nextProps.showLoading, showEmpty: nextProps.showEmpty};
        } else {
            return null;
        }
    }

    render() {
        return (
            <div className={`le-table ${this.props.name} ${this.state.selectable ? 'selectable' : ''}`}>
                <LeTableHeader headerMapping={this.headerMapping} />
                {this.getBody()}
                {this.getEmptyMsg()}
                {this.getLoading()}
                {this.getFooter()}
            </div>
        );
    }
}

LeTable.propTypes = {
    name: propTypes.string.isRequired,
    config: propTypes.object.isRequired,
    data: propTypes.array.isRequired,
    showEmpty: propTypes.bool,
    showLoading: propTypes.bool
};
