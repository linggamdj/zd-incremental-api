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

async function getTickets(nextPageUrl) {
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

        return getTickets(nextPageUrl);
    }

    return response.json();
}

function ticketStatusMap(status_id) {
    let filteredStatus = statuses.find(
        (status) => status.id === Number(status_id)
    );

    return filteredStatus.agent_label;
}

function customFieldMap(fields, field_id) {
    res = "-";

    let filteredStatus = fields.find((field) => field.id === field_id);

    if (filteredStatus.value) {
        let field_value = filteredStatus.value
            .replace(/___/g, "_&_")
            .split("__");

        if (field_id === 20013659669401) {
            res = field_value[field_value.length - 1];
        } else {
            res = filteredStatus.value;
        }
    }

    return res;
}

function insertDatabase(data) {
    client.query(`TRUNCATE TABLE tickets_search`, (err, res) => {
        if (res) {
            console.log("TRUNCATE SUCCESS!");
        } else {
            console.log(err);
            return;
        }
    });

    for (let i = 0; i < data.length; i++) {
        client.query(
            `INSERT INTO tickets_search(ticket_id, status, created_date, category_detail, cabang) VALUES($1, $2, $3, $4, $5) RETURNING *`,
            [
                data[i].id,
                data[i].status === "hold"
                    ? ticketStatusMap(data[i].custom_status_id)
                    : data[i].status,
                data[i].created_at,
                customFieldMap(data[i].custom_fields, 20013659669401),
                customFieldMap(data[i].custom_fields, 20001665446041),
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

async function getAllPages(initUrl) {
    try {
        let nextPage = initUrl;

        while (nextPage) {
            console.log(nextPage);
            const currPageData = await getTickets(nextPage);
            nextPage = currPageData.next_page;
            data = data.concat(currPageData.results);

            if (!nextPage) break;
        }

        insertDatabase(data);
    } catch (error) {
        console.log(error);
    }
}

getAllPages(url);
