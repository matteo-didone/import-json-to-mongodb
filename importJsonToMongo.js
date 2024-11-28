const fs = require('fs');
const { MongoClient } = require('mongodb');

// Configurazione MongoDB
const mongoUri = 'mongodb://localhost:27017';
const dbName = 'historical_events';
const collectionName = 'events';
const jsonFilePath = 'historicalEvents.json';

// Funzione per pulire la descrizione
function cleanDescription(description) {
    return description
        .replace(/\{\{[^}]+\}\}/g, '') // Rimuove i template wiki
        .replace(/<a[^>]*>/g, '') // Rimuove i tag HTML
        .replace(/<\/a>/g, '')
        .replace(/\|[^}]*}/g, '') // Rimuove parti di template rimaste
        .replace(/\s+/g, ' ') // Normalizza gli spazi
        .trim(); // Rimuove spazi iniziali e finali
}

(async function () {
    let client;
    try {
        // Lettura del file JSON come stringa
        console.log('Lettura del file JSON...');
        const fileContent = fs.readFileSync(jsonFilePath, 'utf8');

        // Estrarre tutti gli eventi usando una regex migliorata
        const eventRegex = /"event"\s*:\s*({[^}]*})/g;
        const matches = [...fileContent.matchAll(eventRegex)];

        const events = matches.map(match => {
            try {
                const eventStr = match[1].replace(/\n/g, ' ').replace(/\r/g, '');
                const event = JSON.parse(eventStr);

                // Pulizia e normalizzazione dei dati
                return {
                    date: event.date,
                    description: cleanDescription(event.description),
                    lang: event.lang,
                    granularity: event.granularity,
                    ...(event.category1 && { category1: event.category1 })
                };
            } catch (e) {
                console.log('Skip record con errore di parsing');
                return null;
            }
        }).filter(event => event !== null);

        // Connessione a MongoDB
        client = new MongoClient(mongoUri);
        await client.connect();
        console.log('Connesso a MongoDB');

        const db = client.db(dbName);
        const collection = db.collection(collectionName);

        // Pulizia della collezione esistente
        console.log('Pulizia della collezione esistente...');
        await collection.deleteMany({});

        // Inserimento dei dati in MongoDB
        console.log('Inserimento dati in MongoDB...');
        if (events.length > 0) {
            const result = await collection.insertMany(events);

            // Indicatori di qualità dei dati
            const totalEvents = events.length;
            const validEvents = result.insertedCount;

            console.log('\nStatistiche importazione:');
            console.log(`Eventi totali processati: ${totalEvents}`);
            console.log(`Eventi validi importati: ${validEvents}`);
            console.log(`Percentuale successo: ${((validEvents / totalEvents) * 100).toFixed(2)}%`);

            // Mostra esempi per diverse epoche
            const samples = await collection.aggregate([
                { $sample: { size: 5 } }
            ]).toArray();

            console.log('\nEsempi casuali di eventi importati:');
            console.log(JSON.stringify(samples, null, 2));
        }

    } catch (error) {
        console.error('Errore:', error);
    } finally {
        if (client) {
            await client.close();
            console.log('\nConnessione a MongoDB chiusa.');
        }
    }
})();