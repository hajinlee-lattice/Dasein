angular.module('mainApp.appCommon.utilities.TrackingConstantsUtility', [
])
.service('TrackingConstantsUtility', function () {
    
    // Play Tracking actions
    this.PLAY_TAB_CLICKED = 'PlayTabClicked';
    this.PLAY_LIST_TILE_CLICKED = 'PlayListTileClicked';
    this.PLAY_DETAIL_TILE_CLICKED = 'PlayDetailTileClicked';
    this.TALKING_POINT_EXPANDED = 'TalkingPointExpanded';
    this.TALKING_POINT_COLLAPSED = 'TalkingPointCollapsed';
    
    // Company Snapshot actions
    this.COMPANY_SNAPSHOT_TAB_CLICKED = 'CompanySnapshotTabClicked';
});