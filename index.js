const fs = require('fs');
const path = require('path');
const axios = require('axios');
const saveData = require('./scripts/save-data');
const cheerioScraper = require('./scripts/cheerioScraper');
const puppeteerScraper = require('./scripts/puppeteerScraper');

// VARS
const data_dir = 'data';
const tmp_data_dir = 'tmp-data';
const filename = 'data'; // temp file for data
const urls = ['https://www.gasbuddy.com/GasPrices/British%20Columbia/']; // URL to scrape


async function init(urls, useCheerio) {
	let html;
	// get first url in the list
	const url = urls.shift();
	// clean it up a bit to use as a filename
	const cleanUrl = url.split('//')[1].replace(/\//g, '_');
	const htmlFilepath = `${tmp_data_dir}/${cleanUrl}.html`;
	
	// check if we already have the file downloaded
	const fileExists = fs.existsSync(htmlFilepath);
	
	if (!fileExists) {
		// download the HTML from the web server
		console.log(`Downloading HTML from ${url}...`);
		// fetchDeaths & fetchCases & other files
		html = await axios.get(url);
		
		// save the HTML to disk
		try {
			await fs.promises.writeFile(path.join(__dirname, htmlFilepath), html.data, {flag: 'wx'});
		} catch(err) { 
			console.log(err);
		}
	} else {
		console.log(`Skipping download for ${url} since ${cleanUrl} already exists.`);
	}
	
	// load local copy of html
	html = await fs.readFileSync(htmlFilepath);

	// scrape downloaded file
	const results = await processHTML(html, true);

	// if there's more links, let's do it again!
	if(urls.length > 0) {
		console.log('Downloading next url...');
		downloadHTML(urls, true);
	} else {
		saveData(results, path.join(__dirname, `${data_dir}/${filename}`), 'csv');
	}
}

// scrape & cache results
async function processHTML(html, useCheerio) {
	return (useCheerio) ? await cheerioScraper(html) : await puppeteerScraper(html);
}

// kick isht off!!!
init(urls, true); // set 'useCheerio' to false to run puppeteer




