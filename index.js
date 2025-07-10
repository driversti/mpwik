const axios = require('axios');
const cheerio = require('cheerio');
const { DynamoDBClient, GetItemCommand, PutItemCommand } = require('@aws-sdk/client-dynamodb');

// --- Configuration ---
const URL = 'https://www.mpwik.com.pl/view/planowane';
const DISTRICT_NAME = 'Warszawa URSUS';
const DYNAMO_TABLE_NAME = 'WaterOutagesUrsus';
const DYNAMO_PRIMARY_KEY = 'LATEST_URSUS_OUTAGES';

// Values from Lambda Environment Variables
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

// Initialize DynamoDB Client
const dynamoClient = new DynamoDBClient({ region: process.env.AWS_REGION });

// --- Main Handler ---
exports.handler = async (event) => {
    console.log('Fetching latest water outage data...');
    const html = await fetchHtmlContent();

    if (!html) {
        return { statusCode: 500, body: 'Failed to fetch HTML content.' };
    }

    console.log('Parsing HTML and extracting outage information...');
    const currentOutagesText = parseOutages(html);

    // If there are no outages, stop here.
    if (!currentOutagesText) {
        console.log('No outages found for the specified district.');
        return { statusCode: 200, body: 'No outages found.' };
    }

    console.log('Fetching previous outage data from DynamoDB...');
    const previousOutages = await getPreviousOutages();

    if (currentOutagesText === previousOutages) {
        console.log('No changes detected. Exiting.');
        return { statusCode: 200, body: 'No new water outages.' };
    } else {
        console.log('New outage data detected. Sending notification and updating DynamoDB.');

        // Send notification first
        await sendTelegramMessage(currentOutagesText);

        // Then, save the new state
        await saveNewOutages(currentOutagesText);

        return {
            statusCode: 200,
            body: 'New water outage data found, notified, and saved.',
            newOutages: currentOutagesText
        };
    }
};

// --- Helper Functions ---

async function sendTelegramMessage(text) {
    if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
        console.error('Telegram Bot Token or Chat ID is not configured. Skipping notification.');
        return;
    }

    const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
    const payload = {
        chat_id: TELEGRAM_CHAT_ID,
        text: text,
        parse_mode: 'HTML'
    };

    try {
        await axios.post(url, payload);
        console.log('Successfully sent message to Telegram.');
    } catch (error) {
        console.error('Error sending message to Telegram:', error.message);
    }
}

async function fetchHtmlContent() {
    try {
        const response = await axios.get(URL, {
            headers: {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.6',
                'Cache-Control': 'max-age=0',
                'Connection': 'keep-alive',
                'Cookie': '___utmvm=###########; PHPSESSID=2rh46rpgd7foi3s2537qc73cfs; pokaz_ciasteczka=false; pokaz_syntezator=false; _genesys.widgets.knowledgecenter.state.keys={%22sessionId%22:%22c7364117-d1f4-42c7-9d02-b66405f1c1c6%22}; TBMCookie_5570567948074293652=34367000175207026240cYOKwWuQwStazYDuqcCsrUfz8=',
                'Referer': 'https://www.mpwik.com.pl/view/awarie',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-User': '?1',
                'Sec-GPC': '1',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            }
        });
        return response.data;
    } catch (error) {
        console.error('Error fetching HTML:', error.message);
        return null;
    }
}

function parseOutages(html) {
    const $ = cheerio.load(html);
    let output = '';
    const districts = $('.dzielnica');

    districts.each((i, district) => {
        const header = $(district).find('h3.dzielnicaopen');
        if (header.text().trim().startsWith(DISTRICT_NAME)) {
            const outages = [];
            outages.push(`Wyłączenia planowane:\n\n`);
            $(district).find('.awarie tbody tr:not(.headrow)')
              .each((j, row) => {
                  const cells = $(row).find('td');
                  if (cells.length > 4) {
                      const place = $(cells[0]).contents().first().text().trim();
                      const from = $(cells[1]).text().trim();
                      const to = $(cells[2]).text().trim();

                      let entry =`<strong>${place}</strong> (z ${from} do ${to})\n`;
                      const addressesDiv = $(cells).find('.zbior');
                      if (addressesDiv.length) {
                          const addresses = addressesDiv.html().split('<br>')
                            .map(addr => addr.trim())
                            .filter(addr => addr).sort();
                          addresses.forEach(address => {
                              console.log(`Address: ${address}`);
                              entry += `- ${address}\n`;
                          });
                      }
                      outages.push(entry);
                  }
              });
            output = outages.join('\n');
        }
    });
    return output;
}

async function getPreviousOutages() {
    const params = {
        TableName: DYNAMO_TABLE_NAME,
        Key: {
            'outageKey': { S: DYNAMO_PRIMARY_KEY }
        }
    };
    try {
        const data = await dynamoClient.send(new GetItemCommand(params));
        return data.Item ? data.Item.outageData.S : '';
    } catch (error) {
        console.error('Error getting item from DynamoDB:', error.message);
        return null;
    }
}

async function saveNewOutages(text) {
    const params = {
        TableName: DYNAMO_TABLE_NAME,
        Item: {
            'outageKey': { S: DYNAMO_PRIMARY_KEY },
            'outageData': { S: text },
            'lastUpdated': { S: new Date().toISOString() }
        }
    };
    try {
        await dynamoClient.send(new PutItemCommand(params));
        console.log('Successfully saved new outage data to DynamoDB.');
    } catch (error) {
        console.error('Error saving item to DynamoDB:', error.message);
    }
}
