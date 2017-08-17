"use strict";

var Client = require("pg").Client;
var topojson = require("topojson");
var async = require("async");
var turf = require("turf");
var _ = require("lodash");
var fs = require("fs");
var nm = require('node-monkey')();
nm.attachConsole();
var config = {
    indexedSpatialDataFilePath: "/Users/admin/code/gtfs_polyline/mappedRoutes.json",
    cb: function() {}
}
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

var dbConfig = {
    host: HOST,
    user: USER_NAME,
    password: PASSWORD,
    database:DB_NAME,
    port: PORT
}

var client = new Client(dbConfig);
// client.on('drain', client.end.bind(client)); //disconnect client when all queries are finished
client.connect();

var rawAdjacencies = [],
    rawStops = [],
    rawRoutes = [],
    allGeos = {};

function getShapeID2Coords(cb) {
    client.query(`SELECT shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence FROM ${schema_name}.shapes;`, (err, result) => {
        if(err) {
            return cb(err);
        }
        let shapeIDs2Coords = {},
            currPath,
            prevPoint,
            currPoint,
            distTraveled,
            seqNum,
            lastSeqNum = Number.POSITIVE_INFINITY;
        // console.log(result);
        result.rows.forEach((row, i) => {
            currPoint = turf.point([row.shape_pt_lon, row.shape_pt_lat]);
            seqNum = row.shape_pt_sequence;
            if(seqNum < lastSeqNum) {
                shapeIDs2Coords[row.shape_id] = currPath = [];
                distTraveled = 0;
            }
            else {
                distTraveled += turf.distance(prevPoint, currPoint, "kilometers");
            }
            currPath.push({
                latitude: row.shape_pt_lat,
                longitude: row.shape_pt_lon,
                dist_traveled: distTraveled
            });
            lastSeqNum = seqNum;
            prevPoint = currPoint;
        });
        return cb(null, shapeIDs2Coords)
    });
}

function getTripID2ShapeID(cb) {
    client.query(`SELECT trip_id, shape_id FROM ${schema_name}.trips`, (err, result) => {
        if(err) {
            return cb(err);
        }
        let tripID2ShapeID = {};
        result.rows.forEach((row, i) => {
            if(row.shape_id) {
                tripID2ShapeID[row.trip_id] = row.shape_id;
            }
        });
        return cb(null, tripID2ShapeID);
    });
}

function getTripID2StopIDs(cb) {
    client.query(`SELECT trip_id, stop_id, stop_sequence FROM ${schema_name}.stop_times`, (err, result) => {
        if(err) {
            return cb(err);
        }
        let stopTimesMap = {},
            currStopSeq,
            seqNum;
        // console.log(result.rows);
        result.rows.forEach((row, i) => {
            seqNum = row.stop_sequence;
            // console.log(row);
            if(seqNum === 1) {
                stopTimesMap[row.trip_id] = currStopSeq = [];
            }
            currStopSeq.push(row.stop_id);
        });
        // console.log("stopTimesMap", stopTimesMap);
        return cb(null, stopTimesMap);
    });
}

function getStopIDToCoords(cb) {
    client.query(`SELECT stop_id, stop_lat, stop_lon FROM ${schema_name}.stops`, (err, result) => {
        if(err) {
            return cb(err);
        }
        let stopCoordsMap = {};
        result.rows.forEach((row, i) => {
            stopCoordsMap[row.stop_id] = {
                latitude: row.stop_lat,
                longitude: row.stop_lon
            };
        });
        return cb(null, stopCoordsMap);
    });
}

function getGeoJSPointsForGTFSPoints(gtfsPoints) {
    return gtfsPoints.map(function(pt) {
        return turf.point([pt.longitude, pt.latitude]);
    });
}

function getGeoJSLineSegmentsForGTFSPathWaypoints(waypointCoords) {
    // start @ 2nd waypoint so that the index param to map points to previous waypoint
    return waypointCoords.slice(1, waypointCoords.length).map((curr, index) => {
        let prevCoords = [waypointCoords[index].longitude, waypointCoords[index].latitude],
        currCoords = [curr.longitude, curr.latitude];

        return turf.lineString([prevCoords, currCoords], {
            start_dist_along: waypointCoords[index].dist_traveled
        });
    });
}

function getStopsProjectedToPathSegmentsTable(stop_ids, stopPoints, waypoints, pathSegments) {
    return stopPoints.map(function(stopPt, stopNumber) {
        return pathSegments.map(function(segment, i) {
            let snapped = turf.pointOnLine(segment, stopPt),
                snappedCoords = snapped.geometry.coordinates,
                segmentStartPt = waypoints[i],
                snappedDistTraveled = turf.distance(segmentStartPt, snapped, "kilometers") + segment.properties.start_dist_along,
                deviation = turf.distance(stopPt, snapped, "kilometers");

            return {
                segmentNum: i,
                stop_id: stop_ids[stopNumber],
                stop_coords: stopPt.geometry.coordinates,
                snapped_coords: snappedCoords,
                snapped_dist_along_km: snappedDistTraveled,
                deviation: deviation
            };
        });
    });
}

function trySimpleMinification(table) {
    var possibleOptimal = table.map(function (row) {
        return _.first(_.sortByAll(row, ["deviation", "snapped_dist_along_km"]));
    });
    function invariantCheck (projectedPointA, projectedPointB) {
        return (projectedPointA.snapped_dist_along_km <= projectedPointB.snapped_dist_along_km);
    }

    if (_.every(_.rest(possibleOptimal),
                function (currPossOpt, i) {
                    return invariantCheck(possibleOptimal[i], currPossOpt);
                }))
    {
        return possibleOptimal;
    } else {
        return null;
    }
}
// Finds the stops-to-path fitting with the minimum
//      total squared distance between stops and their projection onto path line segments
//      while maintaining the strong no-backtracking constraint.
//
// O(SW^2) where S is the number of stops, W is the number of waypointCoords in the path.
//
// NOTE: O(S W lg^2 W) is possible by using Willard's range trees on each row to find the optimal
//       cell from the previous row from which to advance.
/**
 *
 * @param {Array} Array of arrays. Rows = projections of stops onto each line segment of the path.
 * @returns {Array|null} an array of the best possible projections for each stop.
 */
function fitStopsToPathUsingLeastSquares (theTable) {

    var bestAssignmentOfSegments;

    // Initialize the first row.
    _.forEach(_.first(theTable), function (cell) {
        cell.cost = (cell.deviation * cell.deviation);
        cell.path = [cell.segmentNum];
    });

    // Do dynamic programing...
    _.forEach(_.rest(theTable), function (stopRow, i) {
        _.forEach(stopRow, function (thisCell) {

            var bestFromPreviousRow = {
                cost : Number.POSITIVE_INFINITY,
            };

            _.forEach(theTable[i], function (fromCell) {
                if ((fromCell.snapped_dist_along_km <= thisCell.snapped_dist_along_km) &&
                    (fromCell.cost < bestFromPreviousRow.cost)) {

                    bestFromPreviousRow = fromCell;
                }
            });

            thisCell.cost = bestFromPreviousRow.cost + (thisCell.deviation * thisCell.deviation);

            if (thisCell.cost < Number.POSITIVE_INFINITY) {
                thisCell.path = bestFromPreviousRow.path.slice(0); // This can be done once.
                thisCell.path.push(thisCell.segmentNum);
            } else {
                thisCell.path = null;
            }
        });
    });


    // Did we find a path that works satisfies the constraint???
    if ((bestAssignmentOfSegments = _.min(_.last(theTable), 'cost').path)) {

        return bestAssignmentOfSegments.map(function (segmentNum, stopIndex) {
            var bestProjection = theTable[stopIndex][segmentNum];

            return {
                segmentNum            : segmentNum                           ,
                stop_id               : bestProjection.stop_id               ,
                stop_coords           : bestProjection.stop_coords           ,
                snapped_coords        : bestProjection.snapped_coords        ,
                snapped_dist_along_km : bestProjection.snapped_dist_along_km ,
                deviation             : bestProjection.deviation             ,
            };
        });

    } else {
        return null;
    }
}

function fitStopsToPath(stop_ids, stopPointCoords, waypointCoords, tripID, shapeID) {
    var stopPoints = getGeoJSPointsForGTFSPoints(stopPointCoords),
        waypoints = getGeoJSPointsForGTFSPoints(waypointCoords),
        pathSegments = getGeoJSLineSegmentsForGTFSPathWaypoints(waypointCoords),
        table = getStopsProjectedToPathSegmentsTable(stop_ids, stopPoints, waypoints, pathSegments),

        originStopID = null,
        destinationStopID = null,

        stopProjections,
        metadata;

    stopProjections = trySimpleMinification(table);
    if(!stopProjections) {
        stopProjections = fitStopsToPathUsingLeastSquares(table);
    }
    if(Array.isArray(stopProjections) && (stopProjections.length)) {
        originStopID = stopProjections[0].stop_id;
        destinationStopID = stopProjections[stopProjections.length - 1].stop_id;
    }

    metadata = {
        __originStopID: originStopID,
        __destinationStopID: destinationStopID,
        __shapeID: shapeID
    };

    if(Array.isArray(stopProjections) && stopProjections.length) {
        return stopProjections.reduce((acc, projection, i) => {
            let prevStopProj = stopProjections[i - 1];
            projection.previous_stop_id = prevStopProj ? prevStopProj.stop_id : null;
            acc[projection.stop_id] = projection;
            return acc;
        }, metadata);
    }
    else {
        return null;
    }
}

function main(err, results) {
    let theIndexedSpatialData,

        projectionsMemoTable = [],
        stopsAndPathsToMemoTableIndex = {},
        tripKeyToMemoTableIndex = {};

    if(err) {
        console.error(err);
        return;
    }

    Object.keys(results.tripID2ShapeID).forEach((tripID) => {
        let shapeID = results.tripID2ShapeID[tripID];

        let tripKey,
            stop_ids = results.tripID2StopIDs[tripID],
            waypointCoords = results.shapeID2Coords[shapeID],
            stopPointCoords,
            stopsToPathKey,
            memoTableIndex,
            stopProjections;

        if(!waypointCoords) {
            return; // if no shape
        }
        tripKey = tripID;

        stopPointCoords = stop_ids.map(function(stopID) {
            return results.stopID2Coords[stopID];
        });

        stopsToPathKey = shapeID + "|" + stop_ids.join("|");

        memoTableIndex = stopsAndPathsToMemoTableIndex[stopsToPathKey];

        if(memoTableIndex !== undefined) { // if these stops/shape have already been projected or nah
            tripKeyToMemoTableIndex[tripKey] = memoTableIndex;
            return;
        }

        stopProjections = fitStopsToPath(stop_ids, stopPointCoords, waypointCoords, tripID, shapeID);

        tripKeyToMemoTableIndex[tripKey] = stopsAndPathsToMemoTableIndex[stopsToPathKey] = (stopProjections && projectionsMemoTable.length);

        if(stopProjections) {
            projectionsMemoTable.push(stopProjections);
        }

    });
    theIndexedSpatialData = {
        shapes: results.shapeID2Coords, // {"shape_id": [{pt, pt}, etc], etc }
        stopProjectionsTable: projectionsMemoTable, // {}
        tripKeyToProjectionsTableIndex: tripKeyToMemoTableIndex
    };

    async.parallel([ outputTheIndexedSpatialData.bind(null, theIndexedSpatialData), outputTheIndexingStatistics ], config.cb);

}
function outputTheIndexingStatistics(cb) {
    // console.log("not outputting indexing stats");
    return cb(null);
}


function outputTheIndexedSpatialData (theIndexedSpatialData, callback) {
    // console.log('Writing the indexed GTFS spatial data to disk.') ;
    // console.log(JSON.stringify(theIndexedSpatialData));
    console.log(theIndexedSpatialData);
    fs.writeFile(config.indexedSpatialDataFilePath, JSON.stringify(theIndexedSpatialData), function (err) {
        if (err) {
            console.error('Error writing the indexed GTFS spatial data to disk.') ;
            return callback(err) ;
        }
        // console.log('Successfully wrote the indexed GTFS spatial data to disk.') ;
        return callback(null) ;
    });
}

var gtfsFileParsers = {
    shapeID2Coords : getShapeID2Coords,
    tripID2ShapeID : getTripID2ShapeID,
    tripID2StopIDs : getTripID2StopIDs,
    stopID2Coords  : getStopIDToCoords,
};

async.parallel(gtfsFileParsers, main);

/*var getAdjacenciesQuery = `SELECT * from ${schema_name}.stop_adjacency ORDER BY id`;
client.query(getAdjacenciesQuery, (err, result) => {
    if(err) {
        console.error(err);
        process.exit();
    }

    // console.log(result.rows[0].route_ids); // [ '289-142', '280-142', '286-142' ]
    rawAdjacencies = result.rows;

});*/
/*
var formatAdjacenciesData = function(rawData) {
    let data = {};

    rawData.forEach((row, i) => {

    });
}*/

// var getStopsQuery = `SELECT * from ${schema_name.name}.stops ORDER BY id`;


/*var gatherGeos = function(data, cb) { // data is adjacencies
    data.forEach((val, i) => {
        let route_ids = val.route_ids,
            toMeshGeos = {},
            getGeoQuery = `SELECT ST_AsGeoJSON(geom) as route_shape, route_id FROM ${schema_name}.routes;`;

        client.query(getGeoQuery, (err, result) => {
            if(err) {
                console.error(err);
                process.exit();
            }
            console.log(JSON.stringify(JSON.parse(result.rows[0].route_shape)));
            // console.log(result);
            let combinedGeos = {
                "type": "FeatureCollection",
                "features": []
            };
            result.rows.forEach((routeGeo, rgi) => {
                let feat = {
                    type: "Feature",
                    geometry: JSON.parse(routeGeo.route_shape),
                    id: routeGeo.route_id
                };
                combinedGeos.features.push(feat);
            });
            // console.log(combinedGeos);
            let topology = topojson.topology({"routes": combinedGeos}, {"property-transform": (f) => {return f.properties}});
            // console.log(topology.objects.routes.geometries);
            // process.exit();
            let newJson = {
                "type": "FeatureCollection",
                "features": [],
                "bbox": topology.bbox,
                "transform": topology.transform
            };
            topology.objects.routes.geometries.forEach((d) => {
                let routeSwap = {
                    "type": "GeometryCollection",
                    "geometries": [d]
                };
                let mesh = topojson.mesh(topology, routeSwap, (a, b) => {return true;});
                let feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": mesh.type,
                        "coordinates": mesh.coordinates
                    }
                }
                newJson.features.push(feature);
            });
            console.log(JSON.stringify(newJson));
            client.end(function(err) {
                if (err) throw err;
            });
        });
        // client.end(function(err) {
        //     if (err) throw err;
        // });
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
        });
    });

    cb();
}



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
