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
    if (request.url().includes('web-events')) {
      console.log('Interceptado:', request.url());
      request.continue(); // Permitir que la solicitud continúe
    } else {
      request.continue(); // Permitir todas las demás solicitudes
    }
  });

  // Manejar respuestas interceptadas
  page.on('response', async (response) => {
    const url = response.url();
    if (url.includes('web-events')) {
      try {
        const jsonResponse = await response.json();
        console.log('Datos de web-events:', JSON.stringify(jsonResponse, null, 2));

        // Enviar a Kafka
        await sendToKafka(jsonResponse);
      } catch (error) {
        console.error('Error al procesar la respuesta de web-events:', error);
      }
    }
  });
}

async function sendToKafka(data) {
  try {
    await producer.connect();

    await producer.send({
      topic: 'waze_traffic',
      messages: [
        { value: JSON.stringify(data) },
      ],
    });

    console.log('Evento enviado a Kafka.');
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
