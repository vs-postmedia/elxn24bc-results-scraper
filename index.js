
import Axios from 'axios';
import Papa from 'papaparse';
import { tidy, arrange, desc, filter, groupBy, leftJoin, max, mutate, pivotWider, select, sum, summarize } from '@tidyjs/tidy';
import saveData from './scripts/save-data.js';

// VARS
const mapOutputFile = `./data/output/current-results-map`;
const seatsOutputFile = `./data/output/current-results-seats`;
// const url = 'https://electionsbcenr.blob.core.windows.net/electionsbcenr/GE-2024-10-19_Candidate.csv'; // URL to scrape
const url = 'https://vs-postmedia-data.sfo2.digitaloceanspaces.com/elxn/elxn2024/elxn24-rest-results.csv';


function joinPartyVotes(leadPartyData, allCandidates) {
	let votesList = [];
	
	// get each party's pop vote for each riding
	leadPartyData.forEach(d => {
		const edVotes = tidy(
			allCandidates,
			filter(ed => ed['Electoral District Code'] === d['Electoral District Code']),
			pivotWider({
				namesFrom: 'Affiliation',
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

async function processMapData(data, leadParty) {
	// create a lookup table of Parties popvote for each riding
	const partyVoteLookup = tidy(
		data,
		mutate({
			'Popular Vote Percentage': d => parseFloat(d['% of Popular Vote'])
		}),
		select([
			'Electoral District Code',
			'Affiliation',
			'Popular Vote Percentage'
		])
	);

	// add back the full list of candidates
	const joinedData = joinPartyVotes(leadParty, partyVoteLookup)

	// save all our data
	await saveData(joinedData, { filepath: mapOutputFile, format: 'csv', append: false });
}

async function processSeatData(data) {
	const seats = tidy(
		data,
		// we only want completed seats
		// filter(d => d['Initial Count Status'] === 'Complete'),
		// 
		groupBy(['Affiliation', 'Initial Count Status']), 
		summarize({
			seats: count('Popular Vote Percentage')
		})
	);


	// save all our data
	await saveData(seats, { filepath: seatsOutputFile, format: 'csv', append: false });
}

async function init(url) {
	// fetch data
	const results = await Axios.get(url)
		.then(resp => Papa.parse(resp.data, { header: true }))
		.catch(err => {
			console.error(err)
		});
	
	// get lead party/candidate for each riding
	const leadParty = getLeadParty(results.data);

	// process data for map
	processMapData(results.data, leadParty);

	// process data for total seat count
	// processSeatData(leadParty);
}

// kick isht off!!!
init(url);




