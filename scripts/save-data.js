import fs from 'fs';
import { Parser } from '@json2csv/plainjs';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';


// create spaces client for Digital Ocean
function getSpacesClient() {
    return new S3Client({
        endpoint: process.env.DO_SPACES_ENDPOINT,
        region: 'us-west-1',
        forcePathStyle: false,
        credentials: {
            accessKeyId: process.env.DO_SPACES_KEY,
            secretAccessKey: process.env.DO_SPACES_SECRET
        }
    });
}

/**
 * Save data locally as JSON or CSV
 */
export async function saveLocal(data, filepath, format) {
    // console.log(`Saving locally: ${filepath}.${format}`);

    try {
        if (format === 'json') {
            fs.writeFileSync(
                `${filepath}.${format}`,
                JSON.stringify(data)
            );
        } else {
            const parser = new Parser({
                withBOM: true
            });

            fs.writeFileSync(
                `${filepath}.${format}`,
                parser.parse(data)
            );
        }

        console.log(`Saved locally: ${filepath}.${format}`);
    } catch (err) {
        console.error('Local save error:', err);
        throw err;
    }
}

/**
 * Save directly to DigitalOcean Spaces
 */
export async function saveRemote(data, filename, format, remoteDir = '') {
    // console.log(`Uploading to Spaces: ${filename}.${format}`);

	// load spaces client
	const spacesClient = getSpacesClient();

    try {
        let content;
        let contentType;

        if (format === 'json') {
            content = JSON.stringify(data);
            contentType = 'application/json';
        } else {
            const parser = new Parser({
                withBOM: true
            });

            content = parser.parse(data);
            contentType = 'text/csv';
        }

        const key = remoteDir
            ? `${remoteDir}/${filename}.${format}`
            : `${filename}.${format}`;


        await spacesClient.send(
            new PutObjectCommand({
                Bucket: process.env.DO_SPACES_BUCKET,
                Key: key,
                Body: content,
                ACL: 'public-read',
                ContentType: contentType
            })
        );

        console.log(`Uploaded to Spaces: ${key}`);

        return key;
    } catch (err) {
        console.error('Spaces upload error:', err);
        throw err;
    }
}

/*** USAGE EXAMPLES ***/
/*
* saveLocal/Remote([data], [local_filepath], [filetype], [remote_filepath])
*
import { saveLocal, saveRemote } from './saveData.js';

// Local only
await saveLocal(
    results,
    './data/file_name',
    'csv'
);

// Spaces only
await saveRemote(
    results,
    'file_name',
    'csv',
    'elections/2026'
);

*********************
*/