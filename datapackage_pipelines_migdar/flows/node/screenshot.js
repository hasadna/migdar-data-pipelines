const puppeteer = require('puppeteer');

(async () => {
    const [url, filename, selector] = process.argv.slice(2);

    const browser = await puppeteer.launch({
        headless: true,
        executablePath: process.env.CHROME_BIN || null,
        args: ['--no-sandbox', '--headless', '--disable-gpu', '--disable-dev-shm-usage']
    });
    const page = await browser.newPage();
    page.setViewport({width: 1300, height: 1200});
    await page.goto(url);
    await page.waitForSelector(selector + ' svg');

    const rect = await page.evaluate(selector => {
        const element = document.querySelector(selector);
        const {x, y, width, height} = element.getBoundingClientRect();
        return {x, y, width, height};
    }, selector);
    await page.screenshot({path: filename, clip: rect});
    await browser.close();
})();