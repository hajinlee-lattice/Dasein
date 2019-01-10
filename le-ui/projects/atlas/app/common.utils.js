export default {
    EscapeFilter: () => {
        'ngInject';

        return window.escape;
    },
    Animation: $animateProvider => {
        'ngInject';

        $animateProvider.classNameFilter(/ngAnimate/);
    }
};

window.HideSpinner = function(selector) {
    angular.element('.inactive-disabled').removeClass('inactive-disabled');
    angular.element(selector || 'section.loading-spinner').remove();
};

window.ShowSpinner = function(LoadingString, selector) {
    // state change spinner
    selector = selector || '#mainContentView';
    LoadingString = LoadingString || '';

    var element = $(selector);

    // jump to top of page during state change
    angular.element(window).scrollTop(0, 0);

    element.children().addClass('inactive-disabled');

    element
        .css({
            position: 'relative'
        })
        .prepend(
            $(
                '<section class="loading-spinner lattice">' +
                    '<h2 class="text-center">' +
                    LoadingString +
                    '</h2>' +
                    '<div class="meter"><span class="indeterminate"></span></div>' +
                    '</section>'
            )
        );

    setTimeout(function() {
        $('section.loading-spinner').addClass('show-spinner');
    }, 1);
};
