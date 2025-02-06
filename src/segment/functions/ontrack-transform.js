// Learn more about destination functions API at
// https://segment.com/docs/connections/destinations/destination-functions

/**
 * Handle track event
 * @param  {SegmentTrackEvent} event
 * @param  {FunctionSettings} settings
 */
async function onTrack(event, settings) {
	// Learn more at https://segment.com/docs/connections/spec/track/

	if (event.event !== 'CMP Submitted')
		throw new EventNotSupported('Only the event CMP Submitted is supported');

	if (event.context.gdpr_privacy === 'accepted') {
		event.event = 'CMP Submitted Accepted';
	} else if (event.context.gdpr_privacy === 'declined') {
		event.event = 'CMP Submitted Declined';
	} else {
		throw new EventNotSupported('gdpr_privacy has to be accepted or declined');
	}
	sendTrack(event, settings);

	if (event.context.gdpr_privacy_advertising === 'accepted') {
		event.event = 'CMP Submitted Advertising Accepted';
	} else if (event.context.gdpr_privacy_advertising === 'declined') {
		event.event = 'CMP Submitted Advertising Declined';
	} else {
		throw new EventNotSupported(
			'gdpr_privacy_advertising has to be accepted or declined'
		);
	}
	sendTrack(event, settings);
}

async function sendTrack(event, settings) {
	console.log(event.event);

	// Learn more at https://segment.com/docs/connections/spec/track/
	let response;

	try {
		response = await fetch(settings.dpWebhookEndpoint, {
			method: 'POST',
			headers: {
				Authorization: settings.authorization,
				'Content-Type': 'application/json'
			},
			body: JSON.stringify(event)
		});
	} catch (error) {
		// Retry on connection error
		throw new RetryError(error.message);
	}

	if (response.status >= 500 || response.status === 429) {
		// Retry on 5xx (server errors) and 429s (rate limits)
		throw new RetryError(`Failed with ${response.status}`);
	}
}

/**
 * Handle identify event
 * @param  {SegmentIdentifyEvent} event
 * @param  {FunctionSettings} settings
 */
async function onIdentify(event, settings) {
	// Learn more at https://segment.com/docs/connections/spec/identify/
	throw new EventNotSupported('identify is not supported');
}

/**
 * Handle group event
 * @param  {SegmentGroupEvent} event
 * @param  {FunctionSettings} settings
 */
async function onGroup(event, settings) {
	// Learn more at https://segment.com/docs/connections/spec/group/
	throw new EventNotSupported('group is not supported');
}

/**
 * Handle page event
 * @param  {SegmentPageEvent} event
 * @param  {FunctionSettings} settings
 */
async function onPage(event, settings) {
	// Learn more at https://segment.com/docs/connections/spec/page/
	throw new EventNotSupported('page is not supported');
}

/**
 * Handle screen event
 * @param  {SegmentScreenEvent} event
 * @param  {FunctionSettings} settings
 */
async function onScreen(event, settings) {
	// Learn more at https://segment.com/docs/connections/spec/screen/
	throw new EventNotSupported('screen is not supported');
}

/**
 * Handle alias event
 * @param  {SegmentAliasEvent} event
 * @param  {FunctionSettings} settings
 */
async function onAlias(event, settings) {
	// Learn more at https://segment.com/docs/connections/spec/alias/
	throw new EventNotSupported('alias is not supported');
}

/**
 * Handle delete event
 * @param  {SegmentDeleteEvent} event
 * @param  {FunctionSettings} settings
 */
async function onDelete(event, settings) {
	// Learn more at https://segment.com/docs/partners/spec/#delete
	throw new EventNotSupported('delete is not supported');
}
