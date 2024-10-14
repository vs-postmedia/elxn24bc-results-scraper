// import fs from 'fs';
import Papa from 'papaparse';
import { promises as fs } from 'fs';

async function saveData(data, options) {
	// not sure why we need this... 🤷‍♂️
	if (data.length === 0) return;
	
	// append or overwrite file
	if (options.append === true) {
		try {
			// covert to csv and append line to file
			await fs.appendFile(
				`${options.filepath}.${options.format}`, 
				`\n${Papa.unparse(data, { header: false })}`,
				(err) => {
					if (err) {
						console.err('Error: ', err);
					} else {
						console.log(`Saved ${options.filepath}`);
					}
				}
			);
		} catch (err) {
			console.error(err);
		}
	} else if (options.append === false) {
		if (options.format === 'js') {
			try {
				// write json file
				await fs.writeFile(`${options.filepath}.js`, `export default ${JSON.stringify(data)}`);
				console.log(`Saved ${options.filepath}`);
			} catch (err) {
				console.error(err);
			} 
		} else
		if (options.format === 'json') {
			try {
				// write json file
				await fs.writeFile(`${options.filepath}.json`, JSON.stringify(data));
				console.log(`Saved ${options.filepath}`);
			} catch (err) {
				console.error(err);
			} 
		} else if (options.format === 'csv') {
			// covert to csv and over/write file
			await fs.writeFile(
				`${options.filepath}.${options.format}`, 
				`${Papa.unparse(data, { header: true })}\n`,
				(err) => {
					if (err) {
						console.err('Error: ', err);
					} else {
						console.log(`Saved ${options.filepath}`);
					}
				}
			);
		}
	}
}

export default saveData;