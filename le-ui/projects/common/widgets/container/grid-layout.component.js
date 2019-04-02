import React, { Component } from "../../react-vendor";
import './grid-layout.scss';

let gridStyle = {};
const containerStyle = {};
class GridLayout extends Component {

    constructor(props) {
        super(props);
        if(props.min){
            gridStyle.gridTemplateColumns = `${'repeat( auto-fill, minmax('}${props.min}${'px, 1fr) )'}`;
        }else if(props.gridStyle){
            gridStyle = Object.assign(gridStyle, props.gridStyle);
        }
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
