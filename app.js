const express = require('express');
const { Client } = require('pg');
const dotenv = require('dotenv');
dotenv.config();

const app = express();
const {
    DB_USER,
    HOST,
    DATABASE,
    PASSWORD,
    PORT,
    DB_PORT,
    DEFAULT_URL,
    MAX_CLICKS_PER_LINK
} = process.env;

const client = new Client({
    user: DB_USER,
    host: HOST,
    database: DATABASE,
    password: PASSWORD,
    port: DB_PORT,
    ssl: true
});

const qtd = MAX_CLICKS_PER_LINK;
const default_url = DEFAULT_URL;

const linear_order = 'SELECT * FROM links WHERE clicks < $1 ORDER BY id ASC LIMIT 1;';
const random_order = 'SELECT id, link, clicks FROM links WHERE clicks < $1 ORDER BY clicks ASC LIMIT 1;';

client.connect(async function (err) {
    if (err) throw err;
    console.log("Connected!");

    // REDIRECT
    app.get('/visit', async (req, res) => {
        const { type } = req.query;

        // Use a transaction to ensure atomicity
        const client = await client.connect();
        try {
            // Begin the transaction
            await client.query('BEGIN');

            // Select a link to redirect based on the type
            const { rows } = await client.query(type == 'random' ? random_order : linear_order, [qtd]);

            console.log(rows);

            if (!rows.length) {
                // No available links, redirect to default_url
                return res.redirect(default_url);
            }

            // Update the clicks for the selected link
            await client.query('UPDATE links SET clicks = $2 WHERE link = $1', [rows[0].link, rows[0].clicks + 1]);

            // Commit the transaction
            await client.query('COMMIT');

            return res.redirect(rows[0].link);
        } catch (error) {
            // Rollback the transaction in case of an error
            await client.query('ROLLBACK');
            throw error;
        } finally {
            // Release the client back to the pool
            client.release();
        }
    });

    app.get('/', async (req, res) => {
        const { type } = req.query;
        const { rows } = await client.query(type == 'random' ? random_order : linear_order, [qtd]);
        return res.json({
            count: rows.length,
            available_links: rows.map(r => r.link)
        });
    });

    // Show All Links and counted clicks
    app.get('/links', async (req, res) => {
        const { rows } = await client.query('SELECT * FROM links;');
        return res.json(rows);
    });

    // ADD MORE LINKS
    app.get('/add', async (req, res) => {
        const { link } = req.query;
        try {
            if (!link) res.redirect(`/links`);
            await client.query('INSERT INTO links(link, clicks) values ($1, 0);', [link]);
        } catch (error) {}
        return res.redirect(`/links`);
    });

    // Remove link
    app.get('/remove', async (req, res) => {
        const { link } = req.query;
        try {
            await client.query('DELETE FROM links WHERE link = $1', [link]);
        } catch (error) {}
        return res.redirect(301, `/links`);
    });

    // Reset clicks
    app.get('/reset', async (req, res) => {
        try {
            await client.query('UPDATE links SET clicks = 0 WHERE TRUE;');
        } catch (error) {}
        return res.redirect(301, `/links`);
    });

    app.listen(PORT, () => {
        console.log(`Example app listening on port ${PORT}`);
    });
});
