
import Axios from 'axios';
import Papa from 'papaparse';
import { tidy, arrange, count, desc, filter, groupBy, leftJoin, max, mutate, pivotWider, select, sum, summarize } from '@tidyjs/tidy';
import saveData from './scripts/save-data.js';

// VARS
const mapOutputFile = `./data/output/current-results-map`;
const seatsOutputFile = `./data/output/current-results-seats`;
// const url = 'https://electionsbcenr.blob.core.windows.net/electionsbcenr/GE-2024-10-19_Candidate.csv'; // URL to scrape
const url = 'https://vs-postmedia-data.sfo2.digitaloceanspaces.com/elxn/elxn2024/elxn24-rest-results.csv';


function assignIndyParty(d, metricName) {
	let party;
	switch (d[metricName]) {
		case 'Independent':
			party = 'Independent/Unaffiliated';
			break;
		case '':
			party = 'Independent/Unaffiliated';
			break;
		case ' ':
			party = 'Independent/Unaffiliated';
			break;
		default:
			party = d[metricName];
			break;
	}

	return party;
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
			})
		);

		votesList = [...votesList, ...edVotes];
	});

	// join party popVote to leading party 
	const joinedData = tidy(
		leadPartyData,
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
		leadingParty: (d) => tidy(
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
		'leadingParty': d =>  d.leadingParty !== undefined ? d.leadingParty['Affiliation'] : null,
		'popVote': d => d.maxVote,
	  }),
	  select([
		'Electoral District Code',
		'Electoral District Name',
		'leadingParty',
		'leadingCandidate',
		'popVote',
		'FinalTotals',
		'Initial Count Status'
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
	const joinedData = joinPartyVotes(leadParty, partyVoteLookup)

	// save all our data
	await saveData(joinedData, { filepath: mapOutputFile, format: 'csv', append: false });
}

async function processSeatData(data, metricName) {
	const seats = tidy(
		data,
		groupBy(['leadingParty']),
		mutate({
			party: d => assignIndyParty(d, metricName)
		}),
		summarize({
			seats: count('party')
		})
	)[0];

	const pivotSeats = tidy(
		seats.seats,
		mutate({
			region: 'B.C.'
		}),
		pivotWider({
			namesFrom: 'party',
			valuesFrom: 'n'
		})
	);

	await saveData(pivotSeats, { filepath: seatsOutputFile, format: 'csv', append: false });
}

async function init(url) {
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
	processSeatData(leadParty, 'leadingParty');

	// process riding level data for map
	processMapData(results, leadParty, 'Affiliation');
}

// kick isht off!!!
init(url);




