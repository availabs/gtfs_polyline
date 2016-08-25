"use strict"

var Client = require("pg").Client;
/*var Promise = require("bluebird");
var utils = require("./utils")
var jf = Promise.promisifyAll(require("jsonfile"));*/

// const runDate = "07232016"

if(process.argv.length != 3) {
    console.log("Format: node handleGeos.js [schema name]");
    process.exit();
}

if(!process.env.GTFS_PASSWORD) {
    console.log("No system variables (GTFS_PASSWORD) set for password");
    process.exit();
}

var DB_NAME = "gtfs";
var USER_NAME = "postgres";
var HOST = "169.226.142.154";
var PORT = "5432";
var PASSWORD = process.env.GTFS_PASSWORD;
var schema_name = process.argv[2];

var config = {
    host: HOST,
    user: USER_NAME,
    password: PASSWORD,
    database:DB_NAME,
    port: PORT
}

var client = new Client(config);
// client.on('drain', client.end.bind(client)); //disconnect client when all queries are finished
client.connect();

var adjacencies = [],
    allGeos = {};

var getAdjacenciesQuery = `SELECT id, route_ids from ${schema_name}.stop_adjacency ORDER BY id`;
client.query(getAdjacenciesQuery, (err, result) => {
    if(err) {
        console.error(err);
        process.exit();
    }

    console.log(result.rows[0].route_ids); // [ '289-142', '280-142', '286-142' ]
    adjacencies = result.rows;
    // console.log(adjacencies);
    gatherGeos(adjacencies, function(){});
});

var gatherGeos = function(data, cb) {
    data.forEach((val, i) => {
        let route_ids = val.route_ids,
            toMeshGeos = {},
            getGeoQuery = `SELECT ST_AsGeoJSON(geom) as route_shape, route_id FROM ${schema_name}.routes;`;

        client.query(getGeoQuery, (err, result) => {
            if(err) {
                console.error(err);
                process.exit();
            }
            console.log(result);
            let combinedGeos = {};
            result.rows.forEach((routeGeo, rgi) => {

            });
            client.end(function (err) {
                if (err) throw err;
            });
        });
        /*route_ids.forEach((route_id) => {
            if(allGeos[route_id]) {
                toMeshGeos[route_id] = allGeos[route_id];
            }
            else {
                let getGeoQuery = `SELECT ST_AsGeoJSON(geom) as route_shape FROM ${schema_name}.routes WHERE route_id='${route_id}';`;
                console.log(getGeoQuery);
                client.query(getGeoQuery, (err, result) => {
                    if(err) { console.error(err); }
                    console.log(result);
                    process.exit();
                })
            }
        });*/
    });

    cb();
}


/*
pool.query(sql)
        .then(res => {
          res.rowCount === 0 ?
            createStateGov(div)
            .then(id => {
              createSubStateGov (div, id)
              .then(() => resolve())
              .catch(err => reject(err))

            })
            .catch(err => reject(err))
          :
            createSubStateGov (div, res.rows[0].id)
              .then(() => resolve())
              .catch(err => reject(err))

        })
*/

/*function loadFile(state, type) {
  var file = "./" + runDate + "/" + state + "/" + type + ".json"
  jf.readFile(file, (err, data) => {
    console.log(data.divisions)
    let divisions = Object.keys(data.divisions).map((div) => {
      let division = data.divisions[div]
      division.ocdId = div
      division.type = utils.ocdGetType(div)
      division.parent = utils.ocdGetParent(div)
      return division
    });
    Promise.map(divisions, function(div){
      return loadDivision(div)
    }, {concurrency: 1})
    .then(divisions => {
      console.log("finished", divisions)
    })
  })
}

function loadOrganization(div) {
  return new Promise(function (resolve, reject) {
    switch(div.type){
      case "state":
        var sql = `select *
        from organization
        where classification = "government"
        and division = "${div.ocdId}"`
        pool.query(sql)
        .then(res => {
          res.rowCount === 0 ?
            createStateGov(div)
            .then(id => {
              createSubStateGov (div, id)
              .then(() => resolve())
              .catch(err => reject(err))

            })
            .catch(err => reject(err))
          :
            createSubStateGov (div, res.rows[0].id)
              .then(() => resolve())
              .catch(err => reject(err))

        })
      break;
      default:
        resolve()
    }
  })
}

function createStateGov (div) {
  return new Promise(function (resolve, reject) {
    console.log(`"create "${div.name} State Government"`)
    var sql = `insert into organization (classification,name,division,"parentId")
      values ("government", "${div.name} State Government", "${div.ocdId}", 4346) RETURNING id`
    pool.query(sql)
    .then(ins => {
      console.log("createStateGov insert", ins)
      resolve(ins.rows[0].id)
    })
    .catch(err => {
      reject(err)
    })
  })
}

function createOrg (div, type, parentId) {
  return new Promise(function (resolve, reject) {
    console.log(`"create "${div.name} State Government"`)
    var sql = `insert into organization (classification,name,division,"parentId")
      values ("government", "${div.name} State Government", "${div.ocdId}", 4346) RETURNING id`
    pool.query(sql)
    .then(ins => {
      console.log("createStateGov insert", ins)
      resolve(ins.rows[0].id)
    })
    .catch(err => {
      reject(err)
    })
  })
}

function createSubStateGov (div, parentId) {
  return new Promise(function (resolve, reject) {
    sql = `select id, classification, "parentId"
        from organization
        where classification in ("executive", "legislature", "sldl", "sldu")
        and division = "${div.ocdId}"`
    pool.query(sql)
    .then(orgs => {
      var reqs = {"executive": {}, "legislature": {}}
      Object.keys(reqs).forEach(type => {
        var currentType = orgs.rows.filter(row => row.type === type)[0]
        if(!currentType){
          reqs[type] = {create: true}
        } else if( currentType["parentId"] !== parentId ) {
          reqs[type] = {update: true}
        } else {
          reqs[type] = {id: currentType.id}
        }
      })
      resolve()
    })
    .catch(err => {
      reject(err)
    })
  })
}

function loadDivision(div) {
  return new Promise(function (resolve, reject) {
    pool.query("select "ocdId", geom from division where "ocdId" = \"" + div.ocdId + "\"")
    .then(res =>{
      if (res.rowCount === 0) {
        let query = `insert into division ("ocdId", name, type, parent)
          values
          ("${div.ocdId}", "${div.name}", "${div.type}", "${div.parent}")`
          console.log("query:", query)
          pool.query(query)
          .then(ins => {
            linkGeometry(div)
            .then(geo =>{
              resolve(div.ocdId)
            })
            .catch(err => {
              reject(err)
            })
          })
          .catch(err => {
            reject(err)
          })

      } else {
        !res.rows[0].geom ?
          linkGeometry(div)
            .then(() => {
              loadOrganization(div).then(() => {
                resolve(div.ocdId)
              }).catch(err => reject(err))
            }).catch(err => reject(err))
        :
          loadOrganization(div).then(() => {
            resolve(div.ocdId)
          }).catch(err => reject(err))

      }
    })
    .catch(err => {
      console.log("load division select error")
      reject(err)
    })
  });
}

function linkGeometry(div) {
  return new Promise(function (resolve, reject) {

    var where_clause = null
    switch(div.type){
      case "state":
          where_clause = `WHERE   geoid = "${utils.ocdGetStatefp(div.ocdId)}"`
      break;

      case "cd":
      case "sldl":
      case "sldu":
          where_clause = `WHERE  geoid = "${utils.ocdGetLegGeoid(div.ocdId)}"`
      break;
    }

    if(!where_clause) {
      reject(`Invalid type ${div.type}`)
    }

    let query = `update division
      set geom = subquery.geom,
      geojson = subquery.geojson
      FROM (
        SELECT ST_SetSRID(geom,26918) as geom, St_AsGeoJSON(ST_SetSRID(geom,4326)) as geojson
        FROM ${div.type}
        ${where_clause}
      ) AS subquery
      WHERE division."ocdId" = "${div.ocdId}";`
    pool.query(query)
    .then(ins => {
      resolve()
    })
    .catch(err => {
      reject(err)
    })
  })
}


loadFile("pa", "state")*/

// Load State
// x 1 - Check for State Division and Create State Division if not exists
// 2 - Join State Division to Geomerty from States Table
// 3 - Check for Organization classification = Government if not create // parent id 4346
// 4 - Check for Organization classification ["executive", "legislative"] if not create with parent from 3
// 5 - For Each Official, check if exists
