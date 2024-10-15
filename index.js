
import Axios from 'axios';
import Papa from 'papaparse';
import { tidy, arrange, count, desc, filter, groupBy, leftJoin, max, mutate, pivotWider, select, sum, summarize } from '@tidyjs/tidy';
import saveData from './scripts/save-data.js';

// VARS
const mapOutputFile = `./data/output/current-results-map`;
const seatsOutputFile = `./data/output/current-results-seats`;
const crdOutputFile = `./data/output/current-results-map-crd`;
const metroOutputFile = `./data/output/current-results-map-metro`;
const url = 'https://electionsbcenr.blob.core.windows.net/electionsbcenr/GE-2024-10-19_Candidate.csv'; // URL to scrape
// const url = 'https://vs-postmedia-data.sfo2.digitaloceanspaces.com/elxn/elxn2024/elxn24-rest-results.csv';

const partyNames = ['Conservative', 'NDP', 'Green', 'Independent', 'Other'];
const crdRidings = ['CWV','ESC','JFM','LAO','LFH','OBG','SAN','SAS','VTB','VTS','MPR'];
const metroRidings = ['ABM','ABS','ABW','BNC','BNE','BNN','BNO','BNS','CHC','CHN','CQB','CQM','DLN','DLS','LLA','LWG','LWI','MAE','MAP','NMC','NVL','NVS','POC','POM','RCB','RCC','RCQ','RCS','SRC','SRD','SRF','SRG','SRN','SUN','SUP','SUR','SUS','SWR','VFV','VHA','VKE','VLA','VLM','VNP','VNQ','VNR','VNS','VNT','VNW','VNY','VTB','VTS','WVC','WVS','FRN'];

function assignIndyParty(d, metricName) {
	let party;
	switch (d[metricName]) {
		case 'Conservative Party':
			party = 'Conservative';
			break;
		case 'BC NDP':
			party = 'NDP';
			break;
		case 'BC Green Party':
			party = 'Green';
			break;
		case 'Independent':
			party = 'Independent';
			break;
		case '':
			party = 'Independent';
			break;
		case ' ':
			party = 'Independent';
			break;
		default:
			party = 'Other';
			break;
	}
	return party;
}

function assignMapColorCategory(party, status) {
	let colorCategory;

	switch (party) {
		case 'Conservative Party':
			colorCategory = status !== 'Complete' ? 'Conservative lead': "Conservative win";
			break;
		case 'BC NDP':
			colorCategory = status !== 'Complete' ? 'NDP lead': "NDP win";
			break;
		case 'BC Green Party':
			colorCategory = status !== 'Complete' ? "Green lead": 'Green win';
			break;
		case 'Independent':
			colorCategory = status !== 'Complete' ? "Independent lead": 'Independent win';
			break;
		default:
			colorCategory = status !== 'Complete' ? "Other lead": 'Other win';
			break;
	}

	return colorCategory;
}

function joinPartyVotes(leadPartyData, allCandidates) {
	let votesList = [];
	
	// get each party's popVote for each riding
	leadPartyData.forEach(d => {
		const edVotes = tidy(
			allCandidates,
			filter(ed => ed['Electoral District Code'] === d['Electoral District Code']),
			pivotWider({
				namesFrom: 'party',
				valuesFrom: 'Popular Vote Percentage'
			}),
			// if there's no results for a party, mark as 0
			mutate({
				...partyNames.reduce((acc, party) => {
					acc[party] = d => d[party] || 0;
					return acc
				}, {})
			})
		);

		votesList = [...votesList, ...edVotes];
	});

	// join party popVote to leading party 
	const joinedData = tidy(
		leadPartyData,
		mutate({
			leadingParty: d => assignIndyParty(d, 'leadingParty'),
		}),
		leftJoin(
			votesList,
			{ by: 'Electoral District Code'}
		)
	);

	return joinedData
}

function getLeadParty(data) {
	const results = tidy(
	  data,
	  // Convert '% of Popular Vote' to a number
	  mutate({
		'Popular Vote Percentage': d => parseFloat(d['% of Popular Vote'])
	  }),
	  // 
	  groupBy(['Electoral District Code', 'Electoral District Name', 'FinalTotals', 'Initial Count Status'], [
		summarize({
		  maxVote: max('Popular Vote Percentage')
		})
	  ]),
	  // Join full dataset back to get top candidate details
	  mutate({
		// name & party of candidate with highest popular vote
		leadingParty: d => tidy(
		  data,
		  filter(r => r['Electoral District Code'] === d['Electoral District Code'] && parseFloat(r['% of Popular Vote']) === d.maxVote
		  ),
		  select([
			'Candidate\'s Ballot Name',
			'Affiliation'
		  ])
		)[0]
	  }),
	  select([
		'Electoral District Code',
		'Electoral District Name',
		'leadingParty',
		'maxVote',
		'FinalTotals',
		'Initial Count Status'
	  ]),
	  mutate({
		'leadingCandidate': d => d.leadingParty !== undefined ? d.leadingParty['Candidate\'s Ballot Name'] : null,
		'leadingParty': d => d.leadingParty !== undefined ? d.leadingParty['Affiliation'] : null,
		'leadingPopVote': d => d.maxVote,
		'colorCategory': d => assignMapColorCategory(d.leadingParty, d['Initital Count Status'])
	  }),
	  select([
		'Electoral District Code',
		'Electoral District Name',
		'leadingParty',
		'leadingCandidate',
		'leadingPopVote',
		'FinalTotals',
		'colorCategory'
	  ])
	);
  
	return results;
}

async function processMapData(data, leadParty, metricName) {
	// create a lookup table of Parties popvote for each riding
	const partyVoteLookup = tidy(
		data,
		mutate({
			party: d => assignIndyParty(d, metricName),
			'Popular Vote Percentage': d => parseFloat(d['% of Popular Vote'])
		}),
		select([
			'Electoral District Code',
			'party',
			'Popular Vote Percentage'
		])
	);

	// add back the full list of candidates
	return joinPartyVotes(leadParty, partyVoteLookup);
}

async function processSeatData(data, metricName) {
	const seats = tidy(
		data,
		groupBy(['leadingParty']),
		mutate({
			'colorCategory': d => assignMapColorCategory(d.leadingParty, d['Initital Count Status']),
			// party: d => assignIndyParty(d, metricName)
		}),
		summarize({
			// seats: count('party')
			seats: count('colorCategory')
		})
	)[0];

	const pivotSeats = tidy(
		seats.seats,
		mutate({
			region: 'B.C.'
		}),
		pivotWider({
			namesFrom: 'colorCategory',
			valuesFrom: 'n'
		})
	);

	return pivotSeats;
}

async function init(url) {
	console.log(`fetching ${url}`)
	// fetch data
	const results = await Axios.get(url)
		.then(resp => Papa.parse(resp.data, { header: true }))
		// filter out empty rows
		.then(data => data.data.filter(d => d['Electoral District Code'].length > 0))
		.catch(err => {
			console.error(err)
		});
	
	// get lead party/candidate for each riding
	const leadParty = getLeadParty(results);
	
	// process data for total seat countz
	const seatData = await processSeatData(leadParty, 'leadingParty');

	// process riding level data for map
	const mapData = await processMapData(results, leadParty, 'Affiliation');

	// filter for metro & crd region maps
	const metroData = mapData.filter(d => metroRidings.includes(d['Electoral District Code']));
	const crdData = mapData.filter(d => crdRidings.includes(d['Electoral District Code']));

	// save all our data
	await saveData(seatData, { filepath: seatsOutputFile, format: 'csv', append: false });
	await saveData(mapData, { filepath: mapOutputFile, format: 'csv', append: false });
	await saveData(metroData, { filepath: metroOutputFile, format: 'csv', append: false });
	await saveData(crdData, { filepath: crdOutputFile, format: 'csv', append: false });
}

// kick isht off!!!
init(url);




