const puppeteer = require('puppeteer');
const { Kafka } = require('kafkajs');

// Kafka Configuration
const kafka = new Kafka({
  clientId: 'waze-scraper',
  brokers: ['localhost:9093']
});

const producer = kafka.producer();

async function scrapeWazeData(page) {
  try {
    // Extract traffic incidents
    const incidents = await page.evaluate(() => {
      // This is a simplified example. You'll need to adjust selectors based on Waze's actual structure
      const incidents = [];
      document.querySelectorAll('.incident-item').forEach(item => {
        incidents.push({
          type: item.getAttribute('data-type'),
          severity: item.getAttribute('data-severity'),
          location: {
            lat: item.getAttribute('data-lat'),
            lng: item.getAttribute('data-lng')
          },
          timestamp: new Date().toISOString(),
          description: item.textContent
        });
      });
      return incidents;
    });
    
    return incidents;
  } catch (error) {
    console.error('Error scraping data:', error);
    return [];
  }
}

async function sendToKafka(incidents) {
  try {
    await producer.connect();
    
    const messages = incidents.map(incident => ({
      key: `${incident.type}-${Date.now()}`,
      value: JSON.stringify(incident)
    }));

    await producer.send({
      topic: 'traffic-incidents',
      messages
    });

    console.log(`Sent ${incidents.length} incidents to Kafka`);
  } catch (error) {
    console.error('Error sending to Kafka:', error);
  }
}

async function main() {
  const browser = await puppeteer.launch({
    headless: false
  });
  
  const page = await browser.newPage();
  await page.goto('https://www.waze.com', {
    waitUntil: 'networkidle0'
  });

  // Set up periodic scraping
  setInterval(async () => {
    const incidents = await scrapeWazeData(page);
    if (incidents.length > 0) {
      await sendToKafka(incidents);
    }
  }, 30000); // Scrape every 30 seconds
}

main().catch(console.error);
