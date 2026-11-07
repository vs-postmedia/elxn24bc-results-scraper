
import path from 'path';
import Axios from 'axios';
import Papa from 'papaparse';
import { tidy, arrange, count, desc, filter, groupBy, leftJoin, max, mutate, pivotWider, select, sum, summarize } from '@tidyjs/tidy';

// load .env from this project's directory, regardless of cwd
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.join(__dirname, '.env') });

// helper scripts
import setTimestamp from './scripts/set-timestamp.js'
import { saveLocal, saveRemote } from './scripts/save-data.js';

// VARS	
const data_dir = 'data/output';
const mapOutputFile = `./data/output/current-results-map`;
const seatsOutputFile = `./data/output/current-results-seats`;
const crdOutputFile = `./data/output/current-results-map-crd`;
const metroOutputFile = `./data/output/current-results-map-metro`;

// const url = 'https://vs-postmedia-data.sfo2.digitaloceanspaces.com/elxn/elxn2026/test-data/GE-2026-10-24_Candidate.csv';
// const url = 'https://vs-postmedia-data.sfo2.digitaloceanspaces.com/elxn/elxn2026/test-data/GE-2026-10-24_Candidate_TEST_VALUES.csv'

const url = 'https://electionsbcenr.blob.core.windows.net/electionsbcenr/GE-2026-10-24_Candidate.csv';


const partyNames = ['NDP', 'Green', 'Independent/Other', 'CentreBC', 'OneBC', 'Conservative'];
const crdRidings = ['CRC', 'CWV','ESC','JFM','LAO','LFH','NOI', 'OBG','SAN','SAS','VTB','VTS'];
const metroRidings = ['ABM','ABS','ABW','BNC','BNE','BNN','BNO','BNS','CHC','CHN','CQB','CQM','DLN','DLS','FRN','LLA','LWG','LWI','MAE','MAP','NMC','NVL','NVS','POC','POM','POR','RCB','RCC','RCQ','RCS','SRC','SRD','SRF','SRG','SRN','SUN','SUP','SUR','SUS','SWR','VFV','VHA','VKE','VLA','VLM','VNP','VNQ','VNR','VNS','VNT','VNW','VNY','WVC','WVS'];

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
		case 'CentreBC':
			party = 'CentreBC';
			break;
		case 'OneBC':
			party = 'OneBC';
			break;
		default:
			party = 'Independent/Other';
			break;
	}
	return party;
}

function assignMapColorCategory(party, status) {
	let colorCategory;

	// no leader (e.g. no votes counted yet)
	if (!party) return null;

	switch (party) {
		case 'Conservative Party':
			colorCategory = status === 'Complete' ? 'Conservative': "Conservative lead";
			// colorCategory = 'Conservative';
			break;
		case 'BC NDP':
			colorCategory = status === 'Complete' ? 'NDP': "NDP lead";
			// colorCategory = 'NDP'
			break;
		case 'BC Green Party':
			colorCategory = status === 'Complete' ? "Green": 'Green lead';
			// colorCategory = 'Green'
			break;
		// case 'Independent':
		// 	colorCategory = status === 'Complete' ? "Independent/Other": 'Independent/Other lead';
		// 	// colorCategory = "Independent"
		// 	break;
		// case 'CentreBC':
		// 	colorCategory = status === 'Complete' ? 'CentreBC': 'CentreBC lead';
		// 	break;
		// case 'OneBC':
		// 	colorCategory = status === 'Complete' ? 'OneBC': 'OneBC lead';
		// 	break;
		default:
			colorCategory = status === 'Complete' ? "Other": 'Other lead';
			// colorCategory = 'Other'
			break;
	}

	return colorCategory;
}

// e.g. "NDP small"; null if there's no leader or margin yet
function assignMovCategory(party, mov) {
	if (!party || mov === null || mov === undefined || !Number.isFinite(mov)) return null;

	const size = mov < 5 ? 'small' : mov <= 15 ? 'medium' : 'large';
	return `${party} ${size}`;
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
			leadingParty: d => d.leadingParty ? assignIndyParty(d, 'leadingParty') : null,
		}),
		mutate({
			movCategory: d => assignMovCategory(d.leadingParty, d.mov)
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
		leadingParty: d => !(d.maxVote > 0) ? undefined : tidy(
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
		mov: d => {
			const rankedVotes = data
				.filter(r => r['Electoral District Code'] === d['Electoral District Code'])
				.map(r => parseFloat(r['% of Popular Vote']))
				.filter(Number.isFinite)
				.sort((a, b) => b - a);
			return rankedVotes.length > 1 ? Number((rankedVotes[0] - rankedVotes[1]).toFixed(1)) : null;
		},
		'statusText': d => d['Initial Count Status'] === 'Complete' ? 'won' : 'leads',
		'colorCategory': d => assignMapColorCategory(d.leadingParty, d['Initial Count Status'])
	  }),
	  select([
		'Electoral District Code',
		'Electoral District Name',
		'leadingParty',
		'leadingCandidate',
		'leadingPopVote',
		'mov',
		'statusText',
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

async function processSeatData(data) {
	// fixed column order; ridings with no leader are not counted
	const headings = partyNames.flatMap(party => [`${party} lead`, `${party} won`]);
	const row = { region: 'B.C.', ...Object.fromEntries(headings.map(h => [h, 0])) };

	data.filter(d => d.colorCategory).forEach(d => {
		const heading = d.colorCategory.endsWith(' lead') ? d.colorCategory : `${d.colorCategory} won`;
		row[heading] += 1;
	});

	return [row];
}

// summary table by party group: ridings elected/leading, total votes & share of votes
// data: raw candidate rows; leadParty: output of getLeadParty()
async function processSeatDataTable(data, leadParty) {
	const groups = ['NDP', 'Conservatives', 'Greens', 'Other'];
	const groupByAffiliation = affiliation => {
		switch (affiliation) {
			case 'BC NDP': return 'NDP';
			case 'Conservative Party': return 'Conservatives';
			case 'BC Green Party': return 'Greens';
			default: return 'Other';
		}
	};

	const rows = Object.fromEntries(groups.map(party => [party, { Party: party, Elect: 0, Lead: 0, Votes: 0 }]));

	// ridings with no leader (e.g. no votes counted yet) are not counted
	leadParty.filter(d => d.leadingParty).forEach(d => {
		const row = rows[groupByAffiliation(d.leadingParty)];
		if (d.statusText === 'won') row.Elect += 1;
		else row.Lead += 1;
	});

	data.forEach(d => {
		const votes = parseInt(String(d['Total Valid Votes']).replace(/,/g, ''), 10);
		if (Number.isFinite(votes)) rows[groupByAffiliation(d.Affiliation)].Votes += votes;
	});

	const totalVotes = groups.reduce((sum, party) => sum + rows[party].Votes, 0);

	return groups.map(party => ({
		Party: party,
		Elect: rows[party].Elect,
		Lead: rows[party].Lead,
		Total: rows[party].Elect + rows[party].Lead,
		Votes: rows[party].Votes,
		'Vote share': totalVotes > 0 ? Number((rows[party].Votes / totalVotes * 100).toFixed(1)) : 0
	}));
}

function saveRemoteFiles(seatData, seatTableData, mapData, crdData, metroData)
 {
		// save to DO
	saveRemote(
		seatTableData,
		'bc-results-seats-table',
		'csv',
		'elxn/elxn2026'
	);
	saveRemote(
		mapData,
		'bc-results-map',
		'csv',
		'elxn/elxn2026'
	);
	saveRemote(
		metroData,
		'metro-results-map',
		'csv',
		'elxn/elxn2026'
	);
	saveRemote(
		crdData,
		'crd-results-map',
		'csv',
		'elxn/elxn2026'
	);
}

async function init(url) {
	console.log(`fetching ${url}`)
	// fetch data
	const results = await Axios.get(url, {})
		.then(resp => Papa.parse(resp.data, { header: true }))
		// filter out empty rows
		.then(data => data.data.filter(d =>
			d['Electoral District Code'].length > 0
			&& d['Candidate\'s Ballot Name'] !== 'Rejected Ballots'
		))
		.catch(err => {
			console.error(err.message);
			process.exit(1);
		});

	// keep raw data record
	saveLocal(results, path.join(__dirname, `${data_dir}/results-raw`), 'csv');
	
	// get lead party/candidate for each riding
	const leadParty = getLeadParty(results);
	
	// process data for total seat countz
	const seatData = await processSeatData(leadParty, 'leadingParty');

	// process party summary table (elected, leads, votes, per cent)
	const seatTableData = await processSeatDataTable(results, leadParty);

	// process riding level data for map
	const mapData = await processMapData(results, leadParty, 'Affiliation');

	// filter for metro & crd region maps
	const metroData = mapData.filter(d => metroRidings.includes(d['Electoral District Code']));
	const crdData = mapData.filter(d => crdRidings.includes(d['Electoral District Code']));

	// save all our data
	await saveLocal(seatData, path.join(__dirname, `${data_dir}/bc-results-seats`), 'csv');
	await saveLocal(seatTableData, path.join(__dirname, `${data_dir}/bc-results-seats-table`), 'csv');
	saveLocal(mapData, path.join(__dirname, `${data_dir}/bc-results-map`), 'csv');
	saveLocal(crdData, path.join(__dirname, `${data_dir}/crd-results-map`), 'csv');
	saveLocal(metroData, path.join(__dirname, `${data_dir}/metro-results-map`), 'csv');
	
	saveRemoteFiles(seatData, seatTableData, mapData, crdData, metroData);

}



// kick isht off!!!
init(url);
