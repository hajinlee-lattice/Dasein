'use strict';

describe('loadjs tests', function () {

    var loader;

    beforeEach(function(){
        loader = new JsLoader();
    });

    describe('loadjs convert path pairs to linked list', function () {
        it('should return null for empty pairs', function () {
            var pairs = [];
            var linkedlist = loader.toLinkedList(pairs);
            expect(linkedlist).toBe(null);
        });

        it('should correctly convert array to linked list', function () {
            var pairs = [
                {path: "a", fallback: "fa"},
                {path: "b", fallback: "fb"},
                {path: "c", fallback: "fc"}
            ];
            var head = loader.toLinkedList(pairs);

            for (var i = 0; i < pairs.length; i++) {
                var pair = head;
                expect(pair.path).toBe(pairs[i].path);
                expect(pair.fallback).toBe(pairs[i].fallback);
                head = head.next;
            }
        });
    });
});