import React, { Component } from "../../react-vendor";
import './grid-layout.scss';

const gridStyle = {};
const containerStyle = {};
class GridLayout extends Component {

    constructor(props) {
        super(props);
        if(props.min){
            gridStyle.gridTemplateColumns = `${'repeat( auto-fill, minmax('}${props.min}${'px, 1fr) )'}`;
        }
        // let columnsGap = this.props.columnsGap ? this.props.columnsGap : 20;
        // let rowsGap = this.props.rowsGap ? this.props.rowsGap : 20;
        // let maxWidth = this.props.maxWidth ? this.props.maxWidth : 1200;
        // let min = this.getColumnMinWidth(maxWidth, columnsGap);
        // gridStyle.gridTemplateColumns = 'repeat( auto-fill, minmax(260px, 1fr) )';
        // gridStyle.gridTemplateRows = 'auto';
        // gridStyle.gridAutoFlow = 'row';
        // gridStyle.rowGap = rowsGap +'px';
        // gridStyle.columnGap = columnsGap + 'px';
        // containerStyle.maxWidth = maxWidth + 'px';
    }

    getColumnMinWidth(maxWidth, columnsGap) {
        let numCol = this.props.numColumns;
        let max = maxWidth;
        let minWidth = (max/numCol) - columnsGap;
        console.log(minWidth);
        return minWidth;
    }

    render() {
        return (
            <div className="le-grid-layout-main">
                <div className="le-grid-layout-container" style={containerStyle}>
                    <div className="le-grid-layout" style={gridStyle}>
                        {this.props.children}
                    </div>
                </div>
            </div>
        );
    }
}

export default GridLayout;
