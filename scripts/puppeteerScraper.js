const puppeteer = require('puppeteer');

async function puppeteerScraper(url) {
	console.log('Launching Puppeteer & loading page...');
	// const browswer = await puppeteer.launch({headless: false});
	const browser = await puppeteer.launch();
	const page = await browser.newPage();
	await page.goto(url, {waitUntil: 'networkidle2'});

	/* 
	Some example scraping...
	const prices = await page.evaluate((selector) => {
		const nodeList = document.querySelectorAll(selector);
		const prices = [...nodeList];
		
		return prices.map(node => node.innerText);
	}, '#prices-table .gb-price');

	const low = prices[0];
	const high = prices[prices.length - 1];
	*/

	console.log('scrape, scrape, scrape');

	// close the page
	console.log('Closing page...');
	await page.close();
	await browser.close();

	return {foo: 'bar'};
}

module.exports = puppeteerScraper;