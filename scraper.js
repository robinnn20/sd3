const puppeteer = require('puppeteer');
const { Kafka } = require('kafkajs');

// Configuración de Kafka
const kafka = new Kafka({
  clientId: 'waze-scraper',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

async function interceptWebEvents(page) {
  // Habilitar la interceptación de solicitudes
  await page.setRequestInterception(true);

  // Manejar solicitudes interceptadas
  page.on('request', (request) => {
    if (request.url().includes('live-map')) {
      console.log('Interceptado:', request.url());
      request.continue(); // Permitir que la solicitud continúe
    } else {
      request.continue(); // Permitir todas las demás solicitudes
    }
  });

  // Manejar respuestas interceptadas
  page.on('response', async (response) => {
    const url = response.url();
    if (url.includes('live-map')) {
      try {
        const jsonResponse = await response.json();
      //  console.log('Datos originales de web-events:', JSON.stringify(jsonResponse, null, 2));

        // Filtrar datos relevantes
        const filteredData = extractRelevantData(jsonResponse);
        console.log('Datos filtrados:', JSON.stringify(filteredData, null, 2));

        // Enviar datos filtrados a Kafka
       // if (filteredData.length > 0) {
        sendToKafka(filteredData);
       // }
      } catch (error) {
        console.error('Error al procesar la respuesta de web-events:', error);
      }
    }
  });
}

// Función para filtrar datos relevantes
function extractRelevantData(data) {
  if (!data || !Array.isArray(data.alerts)) {
    return [];
  }

  return data.alerts.map((alert) => ({
    location: {
      latitude: alert.location?.y || null,
      longitude: alert.location?.x || null,
    },
    severity: alert.reportRating || 'unknown',
    type: alert.type || 'unknown',
    city: alert.city || 'unknown',
    timestamp: new Date().toISOString(),
  }));
}

async function sendToKafka(data) {
  try {
    await producer.connect();

    for (const incident of data) {
      await producer.send({
        topic: 'waze_traffic',
        messages: [
          { value: JSON.stringify(incident) },
        ],
      });
    }

    console.log('Datos filtrados enviados a Kafka.');
  } catch (error) {
    console.error('Error al enviar a Kafka:', error);
  }
}

async function main() {
  const browser = await puppeteer.launch({
    headless: false,
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });

  const page = await browser.newPage();
  try {
    await page.goto('https://www.waze.com', { waitUntil: 'networkidle0' });
    console.log('Página cargada correctamente.');

    // Configurar interceptación de web-events
    await interceptWebEvents(page);

    // Mantener el navegador abierto para continuar interceptando eventos
    console.log('Esperando eventos...');
  } catch (error) {
    console.error('Error en el script:', error);
  } finally {
    // Asegurar que el navegador se cierre correctamente
    process.on('SIGINT', async () => {
      console.log('Cerrando el navegador...');
      await browser.close();
      process.exit();
    });
  }
}

main().catch(console.error);
