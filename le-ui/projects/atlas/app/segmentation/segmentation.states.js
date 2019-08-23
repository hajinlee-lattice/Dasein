import Component from './segmentation.component';

const SegmentationRoutes = {
    parent: 'home',
    name: "segmentationlist",
    url: "/segmentationlist",
    views: {
        'main@': Component
    }
};

export default [SegmentationRoutes];