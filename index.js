const { Storage } = require('@google-cloud/storage');
const { BigQuery } = require('@google-cloud/bigquery');
const axios = require('axios');

const storage = new Storage();
const bucketName = 'pokeapitest'
const fileName = "key.json"

const bigquery = new BigQuery();

async function getApiUrl() {
    const file = storage.bucket(bucketName).file(fileName);
    const defaultApiUrl = "https://pokeapi.co/api/v2/pokemon?offset=0&limit=10"

    try {
        const [contents] = await file.download();
        const apiUrl = JSON.parse(contents.toString()).api_url;
        return apiUrl;
    } catch (error) {
        if (error.code === 404) {
            await saveApiKey(defaultApiUrl);
            return defaultApiUrl
        } else {
            throw error;
        }
    }
}

async function saveApiKey(key) {
    const file = storage.bucket(bucketName).file(fileName);
    const keyObj = { api_url: key };
    await file.save(JSON.stringify(keyObj), {
        contentType: 'application/json',
    });
    console.log(`saved api url to ${fileName}`);
}

async function fetchAllPokemons(apiUrl) {
    console.log("fetching all pokemons...");
    let pokemons = [];

    const response = await axios.get(apiUrl);
    console.log(`fetched ${response.data.results.length} pokemons...`);
    for (const pokemon of response.data.results) {
        console.log(`fetching ${pokemon.name}...`);
        const pokeResponse = await axios.get(pokemon.url);
        pokemons.push(pokeResponse.data);
    }

    console.log(`saving next url...`);
    await saveApiKey(response.data.next);

    return pokemons
}

async function ensureDatasetExists(datasetId) {
    console.log(`ensuring dataset ${datasetId} exists...`);
    const [datasets] = await bigquery.getDatasets();
    const datasetExists = datasets.some(dataset => dataset.id === datasetId);

    if (!datasetExists) {
        await bigquery.createDataset(datasetId);
        console.log(`dataset ${datasetId} created`);
    }
}

async function ensureTableExists(datasetId, tableId, schema) {
    console.log(`ensuring table ${tableId} exists...`);
    const dataset = bigquery.dataset(datasetId);
    const [tables] = await dataset.getTables();
    const tableExists = tables.some(table => table.id === tableId);

    if (!tableExists) {
        await dataset.createTable(tableId, { schema: schema });
        console.log(`table ${tableId} created`);
    }
}

async function savePokemonsToGSC(pokemons) {
    const datasetId = 'pokemon_data';
    const tableId = 'pokemons';

    const schema = {
        fields: [
            { name: 'id', type: 'INTEGER', mode: 'REQUIRED' },
            { name: 'name', type: 'STRING', mode: 'REQUIRED' },
            { name: 'height', type: 'INTEGER', mode: 'REQUIRED' },
            { name: 'weight', type: 'INTEGER', mode: 'REQUIRED' },
        ],
    };

    await ensureDatasetExists(datasetId);

    await ensureTableExists(datasetId, tableId, schema);

    const dataset = bigquery.dataset(datasetId);
    const table = dataset.table(tableId);

    await table.insert((pokemons.map((pokemon) => ({
        id: pokemon.id,
        name: pokemon.name,
        height: pokemon.height,
        weight: pokemon.weight
    }))));

    console.log(`saved ${pokemons.length} pokemons to table ${tableId}`);
}

exports.pokeTestHttp = async (_, res) => {
    console.log("starting poke api test...")
    const apiUrl = await getApiUrl()
    const pokemons = await fetchAllPokemons(apiUrl)
    await savePokemonsToGSC(pokemons)
    res.send('pokemon api test');
}