
const CP_MONTHS = ['Jan.', 'Feb.', 'Mar.', 'Apr.', 'May', 'June', 'July', 'Aug.', 'Sept.', 'Oct.', 'Nov.', 'Dec.'];

function formatTimestamp() {
	const timestampParts = new Intl.DateTimeFormat('en-CA', {
		timeZone: 'America/Vancouver',
		month: 'numeric',
		day: 'numeric',
		hour: 'numeric',
		minute: '2-digit',
		hour12: true
	}).formatToParts();
	
	const timestampValues = Object.fromEntries(
		timestampParts
			.filter(({ type }) => type !== 'literal')
			.map(({ type, value }) => [type, value])
	);
	const month = CP_MONTHS[Number(timestampValues.month) - 1];
	const dayPeriod = timestampValues.dayPeriod.toLowerCase();
	
	return `${month} ${timestampValues.day}, ${timestampValues.hour}:${timestampValues.minute} ${dayPeriod}`;
}

export default formatTimestamp;