const axios = require('axios');
const cheerio = require('cheerio');
const { DynamoDBClient, GetItemCommand, PutItemCommand } = require('@aws-sdk/client-dynamodb');

// --- Configuration ---
const DISTRICT_NAME = 'Warszawa URSUS';
const DYNAMO_TABLE_NAME = 'WaterOutagesUrsus'; // A single table for both outage types

const OUTAGE_TYPES = {
  EMERGENCY: {
    url: 'https://www.mpwik.com.pl/view/awarie',
    dbKey: 'LATEST_URSUS_EMERGENCY_OUTAGES',
    header: 'Wyłączenia awaryjne',
  },
  PLANNED: {
    url: 'https://www.mpwik.com.pl/view/planowane',
    dbKey: 'LATEST_URSUS_PLANNED_OUTAGES',
    header: 'Wyłączenia planowane',
  },
};

// Values from Lambda Environment Variables
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

// Initialize DynamoDB Client
const dynamoClient = new DynamoDBClient({ region: process.env.AWS_REGION });

// --- Main Handler ---
exports.handler = async (event) => {
  console.log('Starting water outage check...');

  // Process emergency outages first, then planned outages.
  await processOutages(OUTAGE_TYPES.EMERGENCY);
  await processOutages(OUTAGE_TYPES.PLANNED);

  console.log('Water outage check finished.');
  return { statusCode: 200, body: 'Check complete.' };
};

// --- Main Processing Function ---
async function processOutages(outageConfig) {
  console.log(`Processing: ${outageConfig.header}`);

  const html = await fetchHtmlContent(outageConfig.url);
  if (!html) {
    console.error(`Failed to fetch HTML for ${outageConfig.header}`);
    return;
  }

  // Use the appropriate parsing function based on outage type
  let currentOutagesText;
  if (outageConfig === OUTAGE_TYPES.EMERGENCY) {
    currentOutagesText = parseEmergencyOutages(html);
  } else {
    currentOutagesText = parseOutages(html);
  }

  if (!currentOutagesText) {
    console.log(`No outages found for "${outageConfig.header}" in the specified district.`);
    return;
  }

  const previousOutages = await getPreviousOutages(outageConfig.dbKey);

  if (currentOutagesText === previousOutages) {
    console.log(`No changes detected for ${outageConfig.header}.`);
  } else {
    console.log(`New data detected for ${outageConfig.header}. Notifying and updating state.`);
    const message = `<b>${outageConfig.header}</b>\n${currentOutagesText}`;
    await sendTelegramMessage(message);
    await saveNewOutages(outageConfig.dbKey, currentOutagesText);
  }
}

// --- Helper Functions ---

async function sendTelegramMessage(text) {
    if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
        console.error('Telegram Bot Token or Chat ID is not configured. Skipping notification.');
        return;
    }
    const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
    const payload = { chat_id: TELEGRAM_CHAT_ID, text: text, parse_mode: 'HTML' };

    try {
        await axios.post(url, payload);
        console.log('Successfully sent message to Telegram.');
    } catch (error) {
        console.error('Error sending message to Telegram:', error.message);
    }
}

async function fetchHtmlContent(url) {
    try {
        const response = await axios.get(url, {
            headers: {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            }
        });
        return response.data;
    } catch (error) {
        console.error(`Error fetching HTML from ${url}:`, error.message);
        return null;
    }
}

// Parse planned outages
function parseOutages(html) {
    const $ = cheerio.load(html);
    let output = '';
    const districts = $('.dzielnica');

    districts.each((i, district) => {
        const header = $(district).find('h3.dzielnicaopen');
        if (header.text().trim().startsWith(DISTRICT_NAME)) {
            const outages = [];
            $(district).find('.awarie tbody tr:not(.headrow)').each((j, row) => {
                const cells = $(row).find('td');
                if (cells.length > 4) {
                    const place = $(cells[0]).contents().first().text().trim();
                    const from = $(cells[1]).text().trim();
                    const to = $(cells[2]).text().trim(); // Corrected index for 'to' date

                    let entry = `<strong>${place}</strong> (od ${from} do ${to})\n`;
                    const addressesDiv = $(cells).find('.zbior');
                    if (addressesDiv.length) {
                        const addresses = addressesDiv.html().split('<br>').map(addr => addr.trim()).filter(addr => addr).sort();
                        addresses.forEach(address => {
                            entry += `- ${address}\n`;
                        });
                    }
                    outages.push(entry);
                }
            });
            outages.sort();
            output = outages.join('\n');
        }
    });
    return output;
}

// Parse emergency outages
function parseEmergencyOutages(html) {
    const $ = cheerio.load(html);
    let output = '';
    
    // Check if there are any emergency outages in the table with id 'awarie'
    const emergencyTable = $('#awarie');
    if (emergencyTable.length) {
        const outages = [];
        
        // Process each row in the table (excluding the header row)
        emergencyTable.find('tbody tr').each((i, row) => {
            const cells = $(row).find('td');
            if (cells.length >= 5) {
                // Extract address from the first column
                const address = $(cells[0]).contents().first().text().trim();
                console.log(`Address: ${address}`);
                
                // Extract outage time from the third column
                const outageTime = $(cells[2]).text().trim();
                console.log(`From: ${outageTime}`);
                
                // Extract expected resolution time from the fourth column
                const expectedResolution = $(cells[3]).text().trim();
                console.log(`To: ${expectedResolution}`);
                
                // Extract status from the fifth column
                const status = $(cells[4]).text().trim();
                console.log(`Status: ${status}`);
                
                // Format the entry
                let statusText = status === 'w toku' ? 'w trakcie' : status;
                let timeInfo = `(od ${outageTime}`;
                if (expectedResolution) {
                    timeInfo += ` do ${expectedResolution})`;
                } else {
                    timeInfo += `, czas usunięcia nieznany)`;
                }
                
                let entry = `<strong>${address}</strong> ${timeInfo} - ${statusText}\n`;
                
                // Extract affected streets/locations from the second column
                const addressesDiv = $(cells[1]).find('.zbior');
                if (addressesDiv.length) {
                    const addresses = addressesDiv.html().split('<br>').map(addr => addr.trim()).filter(addr => addr).sort();
                    addresses.forEach(address => {
                        entry += `- ${address}\n`;
                    });
                }
                
                outages.push(entry);
            }
        });
        
        outages.sort();
        output = outages.join('\n');
    }
    
    return output;
}

async function getPreviousOutages(dbKey) {
    const params = { TableName: DYNAMO_TABLE_NAME, Key: { 'outageKey': { S: dbKey } } };
    try {
        const data = await dynamoClient.send(new GetItemCommand(params));
        return data.Item ? data.Item.outageData.S : '';
    } catch (error) {
        console.error(`Error getting item with key ${dbKey} from DynamoDB:`, error.message);
        return null;
    }
}

async function saveNewOutages(dbKey, text) {
    const params = {
        TableName: DYNAMO_TABLE_NAME,
        Item: {
            'outageKey': { S: dbKey },
            'outageData': { S: text },
            'lastUpdated': { S: new Date().toISOString() }
        }
    };
    try {
        await dynamoClient.send(new PutItemCommand(params));
        console.log(`Successfully saved new data with key ${dbKey} to DynamoDB.`);
    } catch (error) {
        console.error(`Error saving item with key ${dbKey} to DynamoDB:`, error.message);
    }
}
