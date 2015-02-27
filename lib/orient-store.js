/* Copyright (c) 2010-2015 Dan Jenkins, MIT License */
"use strict";

var _ = require('lodash');
var Oriento = require('oriento');
var uuid = require('node-uuid');

var NAME = "orient-store"

module.exports = function(opts) {

  var seneca = this;
  var desc;

  var dbinst = null;
  var server = null;
  var collmap = {};
  var specifications = null;

  function error(err, cb) {
    if( err ) {
      seneca.log.error('entity', err, {store: NAME});
      cb(err);
      return;
    } else {
      return;
    }
  }

  /**
   * configure the store - create a new store specific connection object
   *
   * params:
   * spec - store specific configuration
   * cb - callback
   */

  function configure(spec, cb) {
    specifications = spec;

    server = Oriento({
      host: specifications.host,
      port: specifications.port,
      username: specifications.username,
      password: specifications.password
    });

    server.list().then(function (dbs) {

      var dbExists = dbs.some(function find(db){
        return db.name === specifications.name;
      })

      if (!dbExists) {
        server.create({
          name: specifications.name,
          type: 'graph',
          storage: 'plocal'
        })
        .then(function (db) {
          dbinst = db;
          cb(null, store);
        });
      }else{
        dbinst = server.use(specifications.name);
        cb(null, store);

      }

    });

  }

  var store = {
    name: NAME,

    /**
     * Close the connection
     *
     * params
     * args - optional close command parameters
     * cb - callback
     */

    close: function(args, cb) {
      if(dbinst) {
        dbinst = null;
      }

      if(cb instanceof Function){
        cb(null);
      }
    },

    /**
     * Save the data as specified in the entitiy block on the arguments object
     *
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     */

    save: function(args, cb) {
      var ent = args.ent;

      var update = !!ent.id;

      var tname = tablename(ent);

      if ( !update ) {
        if (ent.id$) {
          ent.id = ent.id$;
        } else {
          ent.id = uuid();
        }
      }

      var entp = makeentp(ent);

      if (update) {
        dbinst.update(tname).set(entp).where({id: entp.id}).scalar().then(function () {
          cb(null, ent);
        }).catch(function(e){
          error(e, cb);
        }).done();
      } else {
        dbinst.insert().into(tname).set(entp).one().then(function (result) {
          cb(null, ent);
        }).catch(function(e){
          error(e, cb);
        }).done();
      }
    },

    /**
     * Load first matching item based on id
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     */

    load: function(args, cb) {
      var qent = args.qent;
      var q = args.q;

      var tname = tablename(qent);

      dbinst.select().from(tname).where({id: q.id}).one().then(function (result){
        var ent = null;

        if (result) {
          ent = makeent(qent, result);
        }

        cb(null, ent);
      }).catch(function(e){
        error(e, cb);
      }).done();

    },

    /**
     * Return a list of objects based on the supplied query, if no query is supplied
     * then return all
     *
     *
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     */

    list: function(args, cb) {
      var qent = args.qent;
      var q = args.q;

      var tname = tablename(qent);

      function query(){
        if (Object.keys(q).length) {
          return dbinst.select().from(tname).where(q).all()
        }
        return dbinst.select().from(tname).all()
      }

      query().then(function (results){
        var list = [];
        results.forEach( function(row){

          //hack until we can figure out how to store arrays and objects and get them converted automatically
          Object.keys(row).forEach(function(key){
            if (typeof row[key] === 'string') {
              //HUGE HACK
              try{
                row[key] = JSON.parse(row[key]);
              }catch(e){
                //dont do anything
              }
            }
          });

          var fent = qent.make$(row);

          list.push(fent);
        });
        cb(null, list);

      }).catch(function(e){
        error(e, cb);
      }).done();
    },

    /**
     * Delete an item
     *
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     * { 'all$': true }
     */

    remove: function(args, cb) {

      var qent = args.qent;
      var q = args.q;

      var tname = tablename(qent);

      function del (){
        if (Object.keys(q).length === 0 || q.all$) {
          return dbinst.delete().from(tname);
        }
        return dbinst.delete().from(tname).where(q);
      }

      del().scalar()
      .then(function (total) {
        cb(null, total)
      }).catch(function(e){
        error(e, cb);
      }).done();
    },

    /**
     * return the underlying native connection object
     */

    native: function(args, cb){
      cb(null, dbinst);
    }

  };

  var meta = seneca.store.init(seneca, opts, store);
  desc = meta.desc;

  seneca.add({
    init:store.name,
    tag:meta.tag
  },function(args, done){
    configure(opts, function(err){
      if ( err ) return seneca.die('store', err, {store: store.name, desc: desc});
      return done();
    })
  });

  return {
    name:store.name,
    tag:meta.tag
  };
}

function tablename(entity) {
  var canon = entity.canon$({object: true});
  return (canon.base ? canon.base + '_' : '') + canon.name;
};

function makeentp(ent) {
  var entp = {};
  var fields = ent.fields$();

  fields.forEach(function(field){
    if ( !_.isDate(ent[field]) && _.isObject(ent[field]) ) {
      entp[field] = JSON.stringify(ent[field]);
    } else {
      entp[field] = ent[field];
    }
  });

  return entp;
};

function makeent(ent,row) {
  var entp;

  var fields = ent.fields$();

  if ( !_.isUndefined(ent) && !_.isUndefined(row) ) {
    entp = {};
    fields.forEach(function(field){
      if ( !_.isUndefined(row[field]) ) {
        if (_.isDate(ent[field])){
          entp[field] = new Date(JSON.parse(row[field]));
        } else if ( _.isObject(ent[field]) ) {
          entp[field] = JSON.parse(row[field]);
        } else {
          entp[field] = row[field];
        }
      }
    });
  }

  return ent.make$(entp);
};
