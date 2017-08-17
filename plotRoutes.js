"use strict";

var Client = require("pg").Client;
var topojson = require("topojson");
var async = require("async");
var turf = require("turf");
var _ = require("lodash");
var fs = require("fs");
var parse = require("json-parse-stream");

// var nm = require('node-monkey')();
// nm.attachConsole();

Array.prototype.remove = function(from, to) {
  var rest = this.slice((to || from) + 1 || this.length);
  this.length = from < 0 ? this.length + from : from;
  return this.push.apply(this, rest);
};

var config = {
    dataFilePath: "/Users/admin/code/gtfs_polyline/cdta_mapped.json",
    cb: function() {}
}

if(process.argv.length != 3) {
    console.log("Format: node plotRoutes.js [schema name]");
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

/*
    {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature", // a single segment that is shared
            "properties": {
                "routes": [The route ids]
            },
            "geometry": {
                "type": "LineString",
                "coordinates": ...
            }
        }, ...
    ]
}
*/
var geo = {};
/*
    [
        { // a single segment that is shared by trip(s)
            "routes": [The route ids],
            "trips": [The trip ids],
            "stops": [
                an ordered array of the stopIds, taken from getStopIDSequences()
            ]
        }
    ]
*/
var segments = [];


/*
    Given any sort of segments w/ IDs (irregardless of ids)
    So that it can work for trips or routes
        (add in the stuff to convert tripIDtoRouteID externally)
    So, eventually, only run on routes (so select the longest trip in the route to represent the route)

    WORKING
*/
function findSegments(sequences, cb) {
    let segments = {};
        /* each individual segment at a time
            {
                "stop1-stop2": [id1, id2, ...],
                ...
            }
        */
    getTripIDtoRouteID(function(err, tripIDtoRouteID) {
        // console.log("got", tripIDtoRouteID);
        Object.keys(sequences).forEach(function(id, i) {
            // console.log(segments);
            let sequence = sequences[id],
                adjacencies = sequenceToAdjacencies(sequence);
                // routeID = tripIDtoRouteID[id];
                // currAdj = adjacecies[0]; // the current single adjacency being considered
                // console.log(routeID);
                // console.log(sequence, " and ", adjacencies);

                for(let adj_i=0; adj_i<adjacencies.length; adj_i++) {
                    let adjacency = adjacencies[adj_i],
                        segKeys = Object.keys(segments);
                    if(segKeys.indexOf(adjacency) === -1) { // if the adjacency isn't already inside the list of segments
                        segments[adjacency] = [];
                    }
                    if(segments[adjacency].length >= 0 && segments[adjacency].indexOf(id) === -1) {
                        segments[adjacency].push(id);
                    }
                }
        });
        return cb(null, segments);
    });
}

/*
    Adjacencies -> shape from one stop to the other
    adjacencies = 
    {
        "stop1-stop2": [tripid1, tripid2, ...],
        ...
    }
*/
function getAdjacecyShapes(adjacencies, cb) {  
    let data = require(config.dataFilePath);
    getTripIDtoRouteID(function(err, tripIDtoRouteID) {
        if(err) {
            console.error(err);
            return cb(err);
        }
        /*
            geo is a geojson of the form:
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "LineString",
                            "coordinates": [...]
                        },
                        "properties": {
                            "stop1": stop_id1
                            "stop2": stop_id2,
                            "tripIds": [...],
                            "routeIds": [...]
                        }
                    }
                ]
            }
        */
        let geo = {
            "type": "FeatureCollection",
            "features": []
        };

        let shapes = data.shapes,
            stopProjectionsTable = data.stopProjectionsTable,
            tripKeyToProjectionsTableIndex = data.tripKeyToProjectionsTableIndex;

        for(let adj_i=0; adj_i<Object.keys(adjacencies).length; adj_i++) {
            /*
                For each adjacency, first find *a* trip that they belong to (so convert route to trip id), then look up in the tripKeyToProj -> stopProjs table to get the two stop's snapped coordinates. Then look up the trip shape (by shapeID) and get the section between those two coordinates.
            */
            let adj = Object.keys(adjacencies)[adj_i], // stop_id1-stop_id2
                tripIds = adjacencies[adj], // [tripid1, tripid2, ...]
                [stop1, stop2] = adj.split("-"),
                firstProj = stopProjectionsTable[tripKeyToProjectionsTableIndex[tripIds[0]]],
                [stop1_seg_i, stop2_seg_i] = [firstProj[stop1].segmentNum, firstProj[stop2].segmentNum],
                rawGeoSlice = shapes[firstProj.__shapeID].slice(stop1_seg_i, stop2_seg_i+1);

            let routeIds = [];
            for(let trip_i=0; trip_i<tripIds.length; trip_i++) {
                let tripID = tripIds[trip_i];
                if(routeIds.indexOf(tripIDtoRouteID[tripID]) === -1) {
                    routeIds.push(tripIDtoRouteID[tripID]);
                }
            }
            let coords = rawGeoSlice.map(function(coord, i) {
                return [coord.longitude, coord.latitude];
            });
            geo.features.push({
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": coords
                },
                "properties": {
                    stop1,
                    stop2, 
                    tripIds,
                    routeIds
                }
            });
        }
        return cb(null, geo);
    });
}

function sequenceToAdjacencies(sequence) {
    // return sequence.join("-");
    let adj = [];
    for(let i=0; i<sequence.length-1; i++) {
        adj.push(sequence[i] + "-" + sequence[i + 1]);
    }
    return adj;
}

function getStopIDSequences(cb) {
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

getStopIDSequences(function(err, result) {
    findSegments(result, function(err, fResult) {
        getAdjacecyShapes(fResult, function(err, geo) {
            console.log(JSON.stringify(geo));
        });
    });
});

function getTripIDtoRouteID(cb) {
    client.query(`SELECT route_id, trip_id FROM ${schema_name}.trips`, (err, result) => {
        if(err) {
            // console.error(err);
            return cb(err);
        }
        let table = {};
        result.rows.forEach((row, i) => {
            table[row.trip_id] = row.route_id;
        });
        // console.log("returning", table);
        return cb(null, table);
    });
}

/*var ya = orderProjectionPoints({
    "123": {
        previous_stop_id: "000",
        stop_id: "123"
    },
    "456": {
        previous_stop_id: "123",
        stop_id: "456"
    },
    "789": {
        previous_stop_id: "456",
        stop_id: "789"
    },
    "abc": {
        previous_stop_id: "789",
        stop_id: "abc"
    },
    "def": {
        previous_stop_id: "abc",
        stop_id: "def"
    },
    "ghi": {
        previous_stop_id: "def",
        stop_id: "ghi"
    },
    __originStopID: "000",
    __destinationStopID: "101112",
});
console.log(ya);*/

function orderProjectionPoints(projection) { // can accept a segment of a projection
    let origStart = projection.__originStopID,
        origEnd = projection.__destinationStopID,
        shapeID = projection.__shapeID,
        pointIDs = Object.keys(projection).filter(function(el, i) {
            return el[0] !== "_"; // only take the IDs
        }),
        orderedPoints = [],
        start,
        end;
    // console.log("pointIDs", pointIDs);
    pointIDs.forEach(function(id, i) {
        let prev = projection[id].previous_stop_id;
        if(prev === null || pointIDs.indexOf(prev) === -1) { // if the previous point doesn't exist in the projection (so this is the first point)
            start = id;
            orderedPoints.splice(0, 0, projection[id]);
        }
        else {
            let indexToInsert = orderedPoints.indexOf(prev) === -1 ? orderedPoints.length: orderedPoints.indexOf(prev)+1;
            orderedPoints.splice(indexToInsert, 0, projection[id]);
        }
    });
    end = orderedPoints[orderedPoints.length - 1].stop_id;
    return {
        start,
        end,
        shapeID,
        orderedPoints
    };
    /*if(pointIDs.indexOf(origStart) !== -1) { // if the proj starting from the beginning
        start = origStart;
        orderedPoints[0] = projection[start];
        pointIDs.forEach(function(id, i) {
            let prev = projection
        });
    }
    else { // iterate through the points until you find one whose prev stop ID isn't in the projection
        pointIDs.forEach(function(id, i) {
            let prev = projection[id].previous_stop_id;
            if(pointIDs.indexOf(prev) === -1) { // if the previous point doesn't exist in the projection (so this is the first point)
                start 
                return true; // exit loop
            }
        });
    }*/
}



