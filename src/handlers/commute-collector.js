const { DynamoDBClient, ScanCommand, PutItemCommand } = require('@aws-sdk/client-dynamodb')
const { marshall, unmarshall } = require('@aws-sdk/util-dynamodb')
const https = require('https')
const moment = require('moment-timezone')
const { v4: uuidv4 } = require('uuid');

exports.commuteCollectorHandler = async () => {

    const dynamodbClient = new DynamoDBClient({ region: 'us-east-2' })

    // Scan our locations dynamoDB table, get all registered locations 
    console.info('Getting locations from dynamoDB')
    let results
    try {
        results = await dynamodbClient.send(new ScanCommand({
            TableName: 'location'
        }))
    }
    catch (error) {
        console.error(`Error getting location data: ${error}`)
    }

    // Is it currently morning or afternoon? Will influence directionality
    const laTime = moment().tz('America/Los_Angeles')
    const isMorning = laTime.hour() < 12

    console.info(`isMorning: ${isMorning}`)

    // Convert dynamoDB record to json, split dwellings and workplaces
    const locations = results['Items'].map(l => unmarshall(l))
    const dwellings = locations.filter(l => !l['is_workplace'])
    const workPlaces = locations.filter(l => l['is_workplace'])

    // In the morning, origin will be dwellings, otherwise workplaces
    const origins = isMorning ? dwellings : workPlaces

    // In the morning, destinations will be workplaces, otherwise dwellings
    const destinations = isMorning ? workPlaces : dwellings

    // For now, we'll gather info on every (dwelling,workplace) pairing indiscriminantly
    // We may want more explicit inclusions/exclusions later
    console.info(`Processing ${dwellings.length * workPlaces.length} combinations total. ${dwellings.length} dwellings and ${workPlaces.length} workplaces`)
    const promises = new Array()
    origins.forEach(origin => {
        destinations.forEach(destination => {
            promises.push(makeGoogleDistanceMatrixRequest(origin, destination))
        })
    })

    // We now have a list of Google commute data + db write key
    return Promise.all(promises).then(commuteData => {

        console.info(`Done querying Google, moving to writes`)

        // Another promise.all sequence for writing to dynamoDB
        return Promise.all(commuteData.map(cd => {
            // Google returns a horrible mess of arrays, pick out the first one, since we're only making one request.
            // Excluse 'status' from data that goes into dynamoDB
            const { status, ...data } = cd['rows'][0]['elements'][0]

            // Construct our dynamoDB item, marshall it (convert it to dynamo DB record), then write it
            console.info(`Writing ${cd['toFrom']} to commute_data`)
            try {
                const dynamoCommuteData = marshall({
                    to_from: cd['toFrom'],
                    to_from_readable: cd['humanReadableToFrom'],
                    uuid: uuidv4(),
                    data,
                    timestamp: laTime.toString()
                })

                return dynamodbClient.send(new PutItemCommand({
                    TableName: 'commute_data',
                    Item: dynamoCommuteData
                }))
            }
            catch (error) {
                reject(error)
            }
        }))
    })
}

const makeGoogleDistanceMatrixRequest = (origin, destination) => {
    console.info(`Google request ${origin['name']} -> ${destination['name']}`)
    return new Promise((resolve, reject) => {
        const url = new URL('https://maps.googleapis.com/maps/api/distancematrix/json')
        url.searchParams.append('units', 'imperial')
        url.searchParams.append('departure_time', 'now')
        url.searchParams.append('origins', origin['coordinates'])
        url.searchParams.append('destinations', destination['coordinates'])
        url.searchParams.append('key', process.env.GOOGLE_API_KEY)

        https.get(url, (resp) => {
            let data = ''

            // A chunk of data has been received.
            resp.on('data', (chunk) => (data += chunk))

            // The whole response has been received. Print out the result.
            resp.on('end', () => {
                if (resp.statusCode != 200) {
                    console.warn(`Google request didn't return 200: ${JSON.parse(JSON.stringify(data))}`)
                    reject();
                }
                const ret = {
                    ...JSON.parse(data),
                    toFrom: `${origin['coordinates']}->${destination['coordinates']}`,
                    humanReadableToFrom: `${origin['name']} -> ${destination['name']}`
                }
                resolve(ret)
            })

            resp.on('error', reject)
        })
    })
}