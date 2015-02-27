/* Copyright (c) 2010-2015 Dan Jenkins, MIT License */

"use strict";

var seneca = require('seneca');
var shared = require('seneca-store-test');

var si = seneca();

si.use('../lib/orient-store.js', {
  name: 'senecatest',
  host: '127.0.0.1',
  username: 'root',
  password: '',
  port: 2424,
  options: {}
});

//REQUIRES THE FOO, MOON_BAR AND PRODUCT CLASSES TO ALREADY EXIST, NEED TO AUTOMATE THAT

si.__testcount = 0;

var testcount = 0;

describe('orient', function(){

  it('basic', function(done){
    testcount++
    shared.basictest(si,done)
  });

  it('close', function(done){
    shared.closetest(si,testcount,done)
  });

});
