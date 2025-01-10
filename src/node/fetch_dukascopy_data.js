const { getHistoricalRates } = require("dukascopy-node");

(async () => {
    try {
        const data = await getHistoricalRates({
            instrument: process.argv[2], // Toma el símbolo desde los argumentos
            dates: {
                from: new Date(process.argv[3]), // Toma la fecha de inicio
                to: new Date(process.argv[4])  // Toma la fecha de fin
            },
            timeframe: process.argv[5], // Timeframe (ej. 'm1', 'tick')
            format: "json", // Formato de salida
            timezone: "America/New_York", // Zona horaria
            useCache: true, // Usar caché
        });
        console.log(JSON.stringify(data)); // Retorna los datos como JSON
    } catch (error) {
        console.error("Error:", error);
        process.exit(1); // Salir con error
    }
})();