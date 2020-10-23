const axios = require('axios');
const colors = require('colors');
const sql = require('mssql');
require('dotenv').config();

const API_URL = {
    "schedule": `https://traintime.lirr.org/api/Departure?loc=NYK`,
    "stations": `https://traintime.lirr.org/api/StationsAll`
};
const REFRESH = true; // Whether or not to run in a loop, or all in one shot
const REFRESH_INTERVAL = 60; // When REFRESH = true, the interval at which to refresh the data in seconds
const REFRESH_INTERVAL_DEVIATION = 10; // When REFRESH = true, the maximum +/- deviation in the number of seconds to wait between requests (feebly making it harder to detect the automated queries)

if (REFRESH_INTERVAL_DEVIATION >= REFRESH_INTERVAL) throw new Error("REFRESH_INTERVAL_DEVIATION must be less than REFRESH_INTERVAL");

const DB_CONFIG = {
    server: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    options: {
        enableArithAbort: true
    }
};

const retrieveAndProcessLIRRStationData = async () => {
    const apiData = await getLIRRStationData();
    await sql.query`
        USE scrapes;
        DELETE FROM lirr_stations;
    `;
    for (var i = 0; i < Object.keys(apiData.data.Stations).length; i++) {
        const currentStation = apiData.data.Stations[Object.keys(apiData.data.Stations)[i]];
        const queryResult = await sql.query`
            USE scrapes;
            INSERT INTO [lirr_stations] (id, name, directions, location, shortname, branch_cd, branch, branch_fare_zone, longitude, latitude, mapurl, accessibility, ticketoffice, waitingroom, info, abbr, loc_cd, milepenn)
            VALUES
            (
                ${Object.keys(apiData.data.Stations)[i]},
                ${currentStation.NAME},
                ${currentStation.DIRECTIONS},
                ${currentStation.LOCATION},
                ${currentStation.SHORTNAME},
                ${currentStation.BRANCH_CD},
                ${currentStation.BRANCH},
                ${currentStation.BRANCH_FARE_ZONE},
                ${currentStation.LONGITUDE},
                ${currentStation.LATITUDE},
                ${currentStation.MAPURL},
                ${currentStation.ACCESSIBILITY},
                ${currentStation.TICKETOFFICE},
                ${currentStation.WAITINGROOM},
                ${currentStation.INFO},
                ${currentStation.ABBR},
                ${currentStation.LOC_CD},
                ${currentStation.MILEPENN}
            )
        `;
        if (queryResult.rowsAffected.length > 0) console.log(`Successfully added station ${currentStation.NAME} (${currentStation.ABBR})`);
        else console.log(`Couldn't add station ${currentStation.NAME} (${currentStation.ABBR})`);
    }
}

const retrieveAndProcessLIRRScheduleData = async () => {
    const apiData = await getLIRRScheduleData();
    for (var i = 0; i < apiData.data.TRAINS.length; i++) {
        var queryResult = await sql.query`
            USE scrapes;
            IF NOT EXISTS
                (
                    SELECT  1
                    FROM    lirr_scrape
                    WHERE   train_id = ${parseInt(apiData.data.TRAINS[i].TRAIN_ID)}
                            AND
                            run_date = ${apiData.data.TRAINS[i].RUN_DATE}
                )
            BEGIN
                INSERT INTO [lirr_scrape] (scheduled_time, train_id, run_date, dest, stops, track, dir, hsf, jam, eta, cd, scrape_time)
                VALUES
                (
                    ${new Date(apiData.data.TRAINS[i].SCHED).toISOString()},
                    ${parseInt(apiData.data.TRAINS[i].TRAIN_ID)},
                    ${apiData.data.TRAINS[i].RUN_DATE},
                    ${apiData.data.TRAINS[i].DEST},
                    ${JSON.stringify(apiData.data.TRAINS[i].STOPS)},
                    ${apiData.data.TRAINS[i].TRACK !== "" ? parseInt(apiData.data.TRAINS[i].TRACK) : null},
                    ${apiData.data.TRAINS[i].DIR},
                    ${apiData.data.TRAINS[i].HSF ? 1 : 0},
                    ${apiData.data.TRAINS[i].JAM ? 1 : 0},
                    ${apiData.data.TRAINS[i].ETA},
                    ${parseInt(apiData.data.TRAINS[i].CD)},
                    ${apiData.responseTime.toISOString()}
                )
            END
            ELSE
            BEGIN
                UPDATE [lirr_scrape]
                SET
                    [scheduled_time] = ${new Date(apiData.data.TRAINS[i].SCHED).toISOString()},
                    [stops] = ${JSON.stringify(apiData.data.TRAINS[i].STOPS)},
                    [track] = ${apiData.data.TRAINS[i].TRACK !== "" ? parseInt(apiData.data.TRAINS[i].TRACK) : null},
                    [dir] = ${apiData.data.TRAINS[i].DIR},
                    [hsf] = ${apiData.data.TRAINS[i].HSF ? 1 : 0},
                    [jam] = ${apiData.data.TRAINS[i].JAM ? 1 : 0},
                    [eta] = ${new Date(apiData.data.TRAINS[i].ETA).toISOString()},
                    [cd] = ${apiData.data.TRAINS[i].CD},
                    [scrape_time] = ${apiData.responseTime}
                WHERE   train_id = ${parseInt(apiData.data.TRAINS[i].TRAIN_ID)}
                        AND
                        run_date = ${apiData.data.TRAINS[i].RUN_DATE}
            END     
        `;
    }
}

const getLIRRScheduleData = async () => {
    console.log('Loading schedule data from LIRR TrainTime API...'.bold)
    return await getLIRRApiData(API_URL.schedule);
}

const getLIRRStationData = async () => {
    console.log('Loading station data from LIRR TrainTime API...')
    return await getLIRRApiData(API_URL.stations);
}

const getLIRRApiData = async (apiUrl) => {
    const apiResponse = await axios.get(apiUrl);
    return {
        responseTime: new Date(Date.now()),
        data: apiResponse.data
    };
}

/** utility functions **/
const getRandomIntegerBetween = (min, max) => Math.floor(Math.random() * (max - min) + min);
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

(async () => {
    console.log(`Opening connection to SQL Server at ${DB_CONFIG.server}...`.bold);
    await sql.connect(DB_CONFIG);
    await retrieveAndProcessLIRRStationData();
    do {
        await retrieveAndProcessLIRRScheduleData();
        await sleep(REFRESH ? ((REFRESH_INTERVAL + getRandomIntegerBetween(-REFRESH_INTERVAL_DEVIATION, REFRESH_INTERVAL_DEVIATION)) * 1000) : 0);
    } while (REFRESH);
    sql.close();
})();