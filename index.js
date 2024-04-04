require("dotenv").config();
const { Client } = require("pg");

// API
const url = `${process.env.ALFA_DOMAIN}${process.env.ALFA_ENDPOINT}`;
const token = process.env.ALFA_TOKEN;

// Store
let data = [];

// DB Conn
const client = new Client({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_DATABASE,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
});

client.connect();

async function getIncremental(nextPageUrl) {
    try {
        const response = await fetch(nextPageUrl, {
            method: "GET",
            headers: {
                "Content-Type": "application/json",
                Authorization: `Basic ${token}`,
            },
        });

        return response.json();
    } catch (error) {
        console.log(error);
    }
}

async function getAllPages(initUrl) {
    try {
        let nextPage = initUrl;

        while (nextPage) {
            const currPageData = await getIncremental(nextPage);
            sortResult(currPageData);
            nextPage = currPageData.next_page;

            if (currPageData.count < 1000) break;
        }

        insertDatabase(data);
    } catch (error) {
        console.log(error);
    }
}

getAllPages(url);

async function sortResult(res) {
    try {
        let sortedRes = res.ticket_events.sort(
            (a, b) => a.ticket_id - b.ticket_id
        );

        sortedRes.push({
            child_events: [],
            next_page: res.next_page,
            count: res.count,
        });

        data = data.concat(sortedRes);
    } catch (error) {
        console.log(error);
    }
}

function insertDatabase(res) {
    client.query(`TRUNCATE TABLE tickets`, (err, res) => {
        if (res) {
            console.log("TRUNCATE SUCCESS!");
        } else {
            console.log(err);
            return;
        }

        client.end;
    });

    res = res.filter((obj) =>
        obj.child_events.some((event) => event.hasOwnProperty("status"))
    );

    let latestObjects = {};

    res.forEach((obj) => {
        latestObjects[obj.ticket_id] = obj;
    });

    let resultArray = Object.values(latestObjects);

    for (let i = 0; i < resultArray.length; i++) {
        let isUpdated = false;
        for (let j = resultArray[i].child_events.length - 1; j >= 0; j--) {
            if (
                resultArray[i].child_events[j].status &&
                resultArray[i].child_events[j].previous_value
            ) {
                isUpdated = true;

                client.query(
                    `INSERT INTO tickets(ticket_id, status) VALUES($1, $2) RETURNING *`,
                    [
                        resultArray[i].ticket_id,
                        resultArray[i].child_events[j].status,
                    ],
                    (err, res) => {
                        if (res) {
                            console.log(res.rows);
                        } else {
                            console.log(err.message);
                        }
                        client.end;
                    }
                );
            }

            if (j === 0 && !isUpdated) {
                client.query(
                    `INSERT INTO tickets(ticket_id, status) VALUES($1, $2) RETURNING *`,
                    [resultArray[i].ticket_id, "new"],
                    (err, res) => {
                        if (res) {
                            console.log(res.rows);
                        } else {
                            console.log(err.message);
                        }
                        client.end;
                    }
                );
            }
        }
    }
}
