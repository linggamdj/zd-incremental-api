require("dotenv").config();
const { Client } = require("pg");

// API
const url = `${process.env.ALFA_DOMAIN}${process.env.ALFA_ENDPOINT}`;
const token = process.env.ALFA_TOKEN;

// DB Conn
const client = new Client({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_DATABASE,
    password: process.env.DB_PASSWORD,
    port: 5432,
});

client.connect();

async function getIncremental() {
    try {
        const response = await fetch(url, {
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

async function sortResult() {
    try {
        let res = await getIncremental();
        res = res.ticket_events.sort((a, b) => a.ticket_id - b.ticket_id);

        return res;
    } catch (error) {
        console.log(error);
    }
}

sortResult()
    .then((res) => {
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
    })
    .catch((err) => console.log(err));
