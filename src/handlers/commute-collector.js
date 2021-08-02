const https = require('https');


exports.commuteCollectorHandler = async () => {

    const url = new URL('https://maps.googleapis.com/maps/api/distancematrix/json');
    url.searchParams.append('units', 'imperial');
    url.searchParams.append('departure_time', 'now');
    url.searchParams.append('origins', '33.964915,-118.363236');
    url.searchParams.append('destinations', '34.032860,-118.458040');
    url.searchParams.append('key', process.env.GOOGLE_API_KEY);

    https.get(url, (resp) => {
        let data = '';

        // A chunk of data has been received.
        resp.on('data', (chunk) => {
            data += chunk;
        });

        // The whole response has been received. Print out the result.
        resp.on('end', () => {
            console.log(JSON.parse(data));
        });
    });
};