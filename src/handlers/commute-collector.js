const { DynamoDBClient, ScanCommand, PutItemCommand } = require('@aws-sdk/client-dynamodb')
const { marshall, unmarshall } = require('@aws-sdk/util-dynamodb')
const https = require('https')
const moment = require('moment-timezone')

exports.commuteCollectorHandler = async () => {

    const dynamodbClient = new DynamoDBClient({ region: 'us-east-2' })

    // Scan our locations dynamoDB table, get all registered locations
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
    const locations = results["Items"].map(l => unmarshall(l))
    const dwellings = locations.filter(l => !l['is_workplace'])
    const workPlaces = locations.filter(l => l['is_workplace'])

    // In the morning, origin will be dwellings, otherwise workplaces
    const origins = isMorning ? dwellings : workPlaces

    // In the morning, destinations will be workplaces, otherwise dwellings
    const destinations = isMorning ? workPlaces : dwellings

    // For now, we'll gather info on every (dwelling,workplace) pairing indiscriminantly
    // We may want more explicit inclusions/exclusions later
    origins.forEach(async origin => {

        destinations.forEach(async destination => {

            // Get commute data (distance, duration, duration_in_traffic) from Google
            const commuteData = await makeGoogleDistanceMatrixRequest(origin, destination)

            // Google returns a horrible mess of arrays, pick out the first one, since we're only making one request.
            // Excluse 'status' from data that goes into dynamoDB
            const { status, ...data } = commuteData["rows"][0]["elements"][0]

            // Construct our dynamoDB item, and marshall it (convert it to dynamo DB record)
            const dynamoCommuteData = marshall({
                to_from: `${origin['coordinates']}->${destination['coordinates']}`,
                data,
                timestamp: laTime
            })

            try {
                await dynamodbClient.send(new PutItemCommand({
                    TableName: 'commute_data',
                    Item: dynamoCommuteData
                }))
            }
            catch (error) {
                console.error(`Error writing commute results to DB: ${error}`)
            }
        })
    })
}

const makeGoogleDistanceMatrixRequest = async (origin, destination) => {
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
            resp.on('end', () => resolve(JSON.parse(data)))

            resp.on('error', reject)
        })
    })
}