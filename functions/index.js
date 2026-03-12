const { onSchedule } = require("firebase-functions/v2/scheduler");
const { onRequest } = require("firebase-functions/v2/https");
const mainScript = require('./main_logic.js');
const priceProcessor = require('./price_processor.js');

// Настройка через v2 API (Cloud Run based)
// В "Blaze" тарифе это работает гораздо лучше.

exports.amazonDailyReport = onSchedule({
    schedule: "0 11 * * *",
    timeZone: "Europe/Kiev",
    memory: "256MiB", // Экономим
    timeoutSeconds: 540,
}, async (event) => {
    console.log('⏰ Будильник сработал! Запускаем бота...');
    try {
        await mainScript.run();
        console.log('✅ Бот закончил работу.');
    } catch (error) {
        console.error('❌ Ошибка бота:', error);
    }
});

const shipmentScript = require('./shipments_logic.js');

// 2. Бот для проверки статусов шипментов (каждые 3 часа)
// Schedule: '0 */3 * * *' = раз в 3 часа (00:00, 03:00, ...)
exports.checkShipments = onSchedule({
    schedule: "0 */3 * * *",
    timeZone: "Europe/Kiev",
    memory: "256MiB",
    timeoutSeconds: 300,
}, async (event) => {
    console.log('📦 Проверка шипментов запущена...');
    try {
        await shipmentScript.run();
        console.log('✅ Проверка шипментов завершена.');
    } catch (error) {
        console.error('❌ Ошибка проверки шипментов:', error);
    }
});

// 3. Webhook для ручной обработки прайс-листов (API-endpoint)
// Сюда можно отправлять POST-запрос с массивом { upc, cost }
exports.analyzePriceList = onRequest({
    memory: "512MiB",
    timeoutSeconds: 540, // 9 хвилин (максимум для HTTP у багатьох браузерах)
    cors: true // Разрешить вызовы из веб-интерфейса
}, async (req, res) => {
    if (req.method !== 'POST') {
        res.status(405).send('Method Not Allowed. Use POST.');
        return;
    }

    try {
        const items = req.body.items;
        if (!Array.isArray(items) || items.length === 0) {
            res.status(400).send({ error: "Invalid payload. Expected { items: [{ upc: '...', cost: 0.00 }] }" });
            return;
        }

        const rawPrepFee = parseFloat(req.body.prepFee);
        const prepFee = isNaN(rawPrepFee) ? 0.50 : rawPrepFee;
        const blacklist = req.body.blacklist || [];
        const mode = req.body.mode || 'full';

        console.log(`📥 Принят запрос на анализ ${items.length} товаров. Prep Fee: $${prepFee}, Mode: ${mode}`);

        // Передаем товары в наш новый модуль обработки
        const results = await priceProcessor.processBatch(items, prepFee, blacklist, mode);

        console.log(`✅ Обработка завершена! Профитных: ${results.profitable.length}, Проблемных: ${results.problematic.length}`);

        res.status(200).send({
            message: "Success",
            stats: { total: items.length, profitable: results.profitable.length, problematic: results.problematic.length },
            results: results
        });
    } catch (error) {
        console.error("❌ Ошибка при обработке прайс-листа:", error);
        res.status(500).send({ error: error.message });
    }
});
// force redeploy Fri Mar  6 16:17:22 EET 2026
