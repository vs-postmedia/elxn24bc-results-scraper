const cheerio = require('cheerio');

async function cheerioScraper(html) {
	let data = {foo: 'bar'};
	const $ = cheerio.load(html);

	// do some scraping
	console.log('scrape, scrape, scrape');

	return data;
}

module.exports = cheerioScraper;