const request = require('request-promise');
const fs = require('fs').promises;
const csv = require('json2csv');
const endpoints = {
    "air-temperature": "https://api.data.gov.sg/v1/environment/air-temperature",
    "rainfall": "https://api.data.gov.sg/v1/environment/rainfall",
    "relative-humidity": "https://api.data.gov.sg/v1/environment/relative-humidity",
    "wind-direction": "https://api.data.gov.sg/v1/environment/wind-direction",
    "wind-speed": "https://api.data.gov.sg/v1/environment/wind-speed"
};
// var standard_input = process.stdin;
// standard_input.setEncoding('utf-8');

// standard_input.on('data', function (data) {
//   // User input exit.
//   var date = data.trim();
//   getWeatherData(date);
// });

function getWeatherData(dateTime, endpoint) {
    return request({
        method: 'GET',
        url: endpoint,
        qs: {
            date_time: dateTime
        },
        headers: {
            'accept': 'application/json'
        },
        timeout: 30000
    });
}

function parseData(rawData) {
    const data = JSON.parse(rawData.toString());
    // console.log(data);
    const stations = data.metadata.stations;
    const type = data.metadata.reading_type;
    const time = data.items[0].timestamp;
    const readings = data.items[0].readings;
    return Promise.resolve({stations, time, readings, type});
}

function buildData(data, parsedData) {
    // console.log(parsedData);
    parsedData.stations.forEach(station => {
        if (!data.stations[station.id]) {
            data.stations[station.id] = {
                id: station.id,
                name: station.name,
                latitude: station.location.latitude,
                longitude: station.location.longitude,
            }
        }
    });

    if (!data.times.includes(parsedData.time)) {
        data.times.push(parsedData.time);
    }

    parsedData.readings.forEach(reading => {
        data.readings.push({
            station_id: reading.station_id,
            time: parsedData.time,
            type: parsedData.type,
            value: reading.value
        });
    });
    return Promise.resolve();
}

function writeToCsv(data) {
    // console.log(data.stations);
    const stationData = csv.parse(Object.values(data.stations), {
    });
    const readingsData = csv.parse(Object.values(data.readings), {
    });
    const id = Date.now();
    fs.open(`./stations-${id}.csv`, "w+")
        .then(file => file.writeFile(stationData));
    fs.open(`./readings-${id}.csv`, "w+")
        .then(file => file.writeFile(readingsData));
    // return readingsData;
}

function init() {
    const total = 500;
    const maxConcurrent = 20;
    const data = {
        stations: {},
        readings: [],
        times: []
    };
    let date = new Date();
    const promises = [];
    let completed = 0;
    let pending = 0;

    function startCycle(data, date) {
        date.setHours(date.getHours() - 1);
        pending += 1;
        let id = completed + pending;
        const promise = cycle(data, date)
            .then(() => {
                completed += 1;
                pending -= 1;
                console.log(`Request ${id} complete, ${completed}/${total}`);
                console.log(`Readings: ${data.readings.length}, Stations: ${Object.values(data.stations).length}`);
                if (completed + pending < total) {
                    return startCycle(data, date);
                }
            })
            .catch(console.error);
        promises.push(promise);
    }

    for (let i = 0; i < maxConcurrent; i++) {
        startCycle(data, date);
    }
    Promise.all(promises).then(() => writeToCsv(data));
}

function cycle(data, dateTime) {
    const promises = Object.values(endpoints).map(endpoint =>
            getWeatherData(dateTime.toISOString().slice(0, -5), endpoint)
            .then(parseData)
            .then(parsedData => {
                buildData(data, parsedData);
                console.log(dateTime, parsedData.time)
                console.log("Delta time: ", dateTime-new Date(parsedData.time));
            })
            .catch(console.error));
    return Promise.all(promises);
}

function test() {
    // const items = 60;
    const endpoint = endpoints["wind-direction"];
    const date = new Date();
    console.log(date);
    const data = {
        stations: {},
        readings: [],
        times: []
    };
    cycle(data, date);
    // const data = new Array(items);
    // const promises = [];
    // for (let i = 0; i < items; i++) {
    //     promises.push(getWeatherData(date.toISOString().slice(0, -5), endpoint)
    //         .then(parseData)
    //         .then(parsedData => {
    //             console.log(i, date, parsedData.time);
    //             data[i] = [new Date(parsedData.time).getMinutes(), parsedData.readings.filter(i => i.station_id === "S107")[0].value];
    //         })
    //         .catch(console.error));
    //     date.setMinutes(date.getMinutes() + 1);
    // }
    // Promise.all(promises)
    //     .then(() => {
    //         console.log(data);
    //     })
}

test();
//
// init();
