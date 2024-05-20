const { Client } = require("pg");
require("dotenv").config();

// Local JSON
const statuses = require("./data/ticket_custom_status.json");

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
    const response = await fetch(nextPageUrl, {
        method: "GET",
        headers: {
            "Content-Type": "application/json",
            Authorization: `Basic ${token}`,
        },
    });

    if (response.status === 429) {
        const secondsToWait = Number(response.headers.get("retry-after"));
        await new Promise((resolve) =>
            setTimeout(resolve, secondsToWait * 1000)
        );

        return getIncremental(nextPageUrl);
    }

    return response.json();
}

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

function concatChildEvents(res) {
    const eventsMap = new Map();

    res.forEach((item) => {
        const { ticket_id, created_at, child_events } = item;

        if (eventsMap.has(ticket_id)) {
            eventsMap.get(ticket_id).child_events.push(...child_events);
        } else {
            eventsMap.set(ticket_id, {
                ticket_id,
                created_at,
                child_events: [...child_events],
            });
        }
    });

    const result = Array.from(eventsMap.values());

    return result;
}

function ticketStatusMap(ticketId) {
    let filteredStatus = statuses.find(
        (status) => status.id === Number(ticketId)
    );

    return filteredStatus.status_category;
}

function insertDatabase(res) {
    client.query(`TRUNCATE TABLE tickets_example`, (err, res) => {
        if (res) {
            console.log("TRUNCATE SUCCESS!");
        } else {
            console.log(err);
            return;
        }
    });

    res = res.filter((obj) =>
        obj.child_events.some(
            (event) =>
                event.hasOwnProperty("status") ||
                event.hasOwnProperty("custom_status_id") ||
                event.hasOwnProperty("custom_ticket_fields")
        )
    );

    let resultArray = concatChildEvents(res);

    for (let i = 0; i < resultArray.length; i++) {
        let isStatus = false;
        let isCustomStatusId = false;
        let isCategoryDetail = false;
        let isCabang = false;

        let storeChild = {
            ticket_id: resultArray[i].ticket_id,
            status: "",
            custom_status_id: "",
            created_at: resultArray[i].created_at,
            category_detail: null,
            cabang: null,
        };

        for (let j = resultArray[i].child_events.length - 1; j >= 0; j--) {
            if (
                !isCustomStatusId &&
                resultArray[i].child_events[j].custom_status_id &&
                resultArray[i].child_events[j].previous_value
            ) {
                isCustomStatusId = true;
                storeChild.custom_status_id = ticketStatusMap(
                    resultArray[i].child_events[j].custom_status_id
                );
            }

            if (
                !isStatus &&
                resultArray[i].child_events[j].status &&
                resultArray[i].child_events[j].previous_value
            ) {
                isStatus = true;
                storeChild.status = resultArray[i].child_events[j].status;
            }

            if (
                !isCategoryDetail &&
                resultArray[i].child_events[j].custom_ticket_fields &&
                resultArray[i].child_events[j].custom_ticket_fields[
                    "20013659669401"
                ]
            ) {
                isCategoryDetail = true;
                storeChild.category_detail =
                    resultArray[i].child_events[j].custom_ticket_fields[
                        "20013659669401"
                    ].split("__")[6];
            }

            if (
                !isCabang &&
                resultArray[i].child_events[j].custom_ticket_fields &&
                resultArray[i].child_events[j].custom_ticket_fields[
                    "20001665446041"
                ]
            ) {
                isCabang = true;
                storeChild.cabang =
                    resultArray[i].child_events[j].custom_ticket_fields[
                        "20001665446041"
                    ];
            }

            if (j === 0 && (isStatus || isCustomStatusId)) {
                client.query(
                    `INSERT INTO tickets_example(ticket_id, status, created_date, category_detail, cabang) VALUES($1, $2, $3, $4, $5) RETURNING *`,
                    [
                        storeChild.ticket_id,
                        storeChild.custom_status_id || storeChild.status,
                        storeChild.created_at,
                        storeChild.category_detail,
                        storeChild.cabang,
                    ],
                    (err, res) => {
                        if (res) {
                            console.log(res.rows);
                        } else {
                            console.log(err.message);
                        }
                    }
                );
            }

            if (j === 0 && !isCustomStatusId && !isStatus) {
                console.log(storeChild);
                client.query(
                    `INSERT INTO tickets_example(ticket_id, status, created_date, category_detail, cabang) VALUES($1, $2, $3, $4, $5) RETURNING *`,
                    [
                        storeChild.ticket_id,
                        "new",
                        storeChild.created_at,
                        storeChild.category_detail,
                        storeChild.cabang,
                    ],
                    (err, res) => {
                        if (res) {
                            console.log(res.rows);
                        } else {
                            console.log(err.message);
                        }
                    }
                );
            }
        }
    }
}

async function getAllPages(initUrl) {
    try {
        let nextPage = initUrl;

        while (nextPage) {
            const currPageData = await getIncremental(nextPage);
            sortResult(currPageData);
            nextPage = currPageData.next_page;
            console.log(nextPage);

            if (currPageData.count < 1000) break;
        }

        data = data.sort((a, b) => a.ticket_id - b.ticket_id);

        insertDatabase(data);
    } catch (error) {
        console.log(error);
    }
}

getAllPages(url);
