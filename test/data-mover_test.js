'use strict';

var data_mover = require('../lib/data-mover.js');


var expect = require('chai').expect;

var path = require('path');

require('chai').should();


describe('messageParser', function () {
    describe("module", function () {
        it("should load", function () {
            expect(data_mover).not.to.be.equal(null);
            expect(data_mover).to.be.a('object');

        });
    });

    describe("run", function () {
        it("should run", function () {

            var sqlite3 = require('sqlite3').verbose();
            var db = new sqlite3.Database('../old-data/ecm.db',sqlite3.OPEN_READWRITE,function(err){
                if (err) throw err;

                db.each("SELECT * FROM users", function(err, row) {
                    console.dir(row);
                });
            });





        });
    });


});