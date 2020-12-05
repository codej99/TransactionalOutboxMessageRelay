'use strict';
const AWS = require("aws-sdk");
const kafka = require("kafka-node");
const mysql = require('mysql');
AWS.config.update({
    region: process.env.REGION
})

const createConnection = () => {
    return mysql.createConnection({
        host: process.env.DB_HOST,
        port: process.env.DB_PORT,
        user: process.env.DB_USER,
        password: process.env.DB_PASSWD,
        database: process.env.DB_NAME
    });
}

const publishOutbox = () => {
    const connection = createConnection();
    const sql = `SELECT
                    id,
                    aggregate_id,
                    aggregate_type,
                    event_type,
                    payload,
                    created_at_datetime
                FROM outbox
                ORDER BY id asc`;
    connection.query(sql, function (err, result) {
        if (result) {
            pushToKafka(result);
        } else {
            console.error(`publishOutbox failed - ${err}`);
        }
    });
    connection.end(function (err) {
    });
};

const pushToKafka = (rows) => {
    const Producer = kafka.Producer,
        client = new kafka.KafkaClient({kafkaHost: process.env.KAFKA_BROKER}),
        producer = new Producer(client, {partitionerType: 3});

    let payloads = [];
    for (let i = 0; i < rows.length; i++) {
        rows[i].payload = JSON.parse(rows[i].payload);
        console.log(rows[i].id + " : " + JSON.stringify(rows[i]));
        payloads.push({
            topic: process.env.KAFKA_TOPIC,
            messages: JSON.stringify(rows[i]),
            key: rows[i].aggregate_id
        })
    }
    producer.on("ready", function () {
        producer.send(payloads, function (err, result) {
            if (result) {
                console.info(`[SUCCESS] publishKafka - ${JSON.stringify(result)}`);
                deleteOutbox(rows);
            } else {
                console.error(`publishKafka failed - ${err}`);
            }
            producer.close();
        })
    });
    producer.on("error", function (err) {
        console.error(`Unknown Error Occured : ${err}`);
    });
};

const deleteOutbox = (rows) => {
    const connection = createConnection();
    for (let i = 0; i < rows.length; i++) {
        const sql = `DELETE FROM outbox WHERE id=${rows[i].id}`;
        connection.query(sql, function (err, result) {
            if (result)
                console.info(`[SUCCESS] deleteOutbox - ${JSON.stringify(result)}`);
            else
                console.error(`deleteOutbox failed - ${err}`);
        });
    }
    connection.end(function (err) {
    });
};

module.exports.main = event => {
    publishOutbox();
};
