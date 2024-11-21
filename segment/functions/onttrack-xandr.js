// This is a beta feature, subject to Segment's Beta Terms and Conditions (https://segment.com/legal/first-access-beta-preview/)
// Learn more at https://segment.com/docs/connections/functions/destination-functions/#batching-the-destination-function-beta
//const ttl = 10 * 60 * 1000;
//let cache = { default: { ts: 1000, xid: null } };

const profilesFetch = async (url, settings) => {
	try {
		res = await fetch(url, {
			headers: new Headers({
				Authorization: 'Basic ' + btoa(settings.personasToken + ':'),
				'Content-Type': 'application/json'
			}),
			method: 'get'
		});
		//console.log(JSON.stringify(res));
		//return res.json();
	} catch (error) {
		// Retry on connection error
		console.log('Error calling Profile Api:' + error.message);
		throw new RetryError(error.message);
	}

	if (res.status >= 500 || res.status == 429) {
		// Retry on 5xx (server errors) and 429s (rate limits)
		throw new RetryError(`[ERROR] Status ${res.status} received from ${url}`);
	}
	const res_log = await res.json();
	console.log(`raw res from API ${JSON.stringify(res_log)}`);
	return res_log;
};

//  Fetch Xandr_id by AnonymousId
const getExternalIdsAnonymousId = async (anonymousId, settings) => {
	const profilesEndpoint = `https://profiles.segment.com/v1/spaces/${settings.spaceId}/collections/users/profiles/anonymous_id:${anonymousId}/external_ids?include=xandr_id,android.idfa,ios.idfa`;
	return [await profilesFetch(profilesEndpoint, settings)];
};

//  Fetch Xandr_id by UserId
const getExternalIdsUserId = async (userId, settings) => {
	const profilesEndpoint = `https://profiles.segment.com/v1/spaces/${settings.spaceId}/collections/users/profiles/user_id:${userId}/external_ids?include=xandr_id,android.idfa,ios.idfa`;
	return [await profilesFetch(profilesEndpoint, settings)];
};

/**
 * Handle a batch of events
 * @param  {SegmentEvent} event
 * @param  {FunctionSettings} settings
 */
async function onTrack(event, settings) {
	const { anonymousId, userId, properties } = event;
	let externalIdsFromAPI;

	try {
		externalIdsFromAPI = userId
			? await getExternalIdsUserId(userId, settings)
			: await getExternalIdsAnonymousId(anonymousId, settings);
	} catch (error) {
		throw new RetryError(
			'[ERROR] Error getting externalIds from Profile API ' + error
		);
	}
	console.log(externalIdsFromAPI);
	console.log(JSON.stringify(externalIdsFromAPI));
	console.log('Profile API called. Parsing ExternalIds next.');

	const userAudiences = externalIdsFromAPI
		.filter(({ data }) => Boolean(data))
		.map(externalIds => {
			if (externalIds.error != null) {
				throw new RetryError(
					'[ERROR] Error parsing externalIds ' + JSON.stringify(externalIds)
				);
			}
			// loop through external IDs to get IDFA values
			const xandrIds = externalIds.data
				.filter(exId => exId.type.includes('xandr_id'))
				.map(exId => exId.id);

			const idfas = externalIds.data
				.filter(exId => exId.type.includes('ios.idfa'))
				.map(exId => exId.id);

			const aaids = externalIds.data
				.filter(exId => exId.type.includes('android.idfa'))
				.map(exId => exId.id);

			// If the user do not have xandrIds
			//if (xandrIds.length < 1) {
			//	return;
			//}

			// Get the audience
			const audience_key = properties.audience_key;

			const audiences = new Map();
			audiences.set(audience_key, properties[audience_key]);

			const userAudience = new UserAudience(
				xandrIds,
				Object.fromEntries(audiences),
				idfas,
				aaids
			);
			return userAudience;
		})
		.filter(ids => Boolean(ids));

	console.log('ExternalIDs parsed. Preparing to send to AdIt microservice.');
	const body = { userAudiences: userAudiences };
	console.log('Before sending userAudiences ' + JSON.stringify(body));

	// Send to AdIt Microservice
	await sendRequestToMs(body, settings);
}

function UserAudience(xandrIds, audiences, idfas, aaids) {
	this.xandrIds = xandrIds;
	this.audiences = audiences;
	this.idfas = idfas;
	this.aaids = aaids;
}

async function sendRequestToMs(userAudiences, settings) {
	try {
		const endpoint = settings.aditEndpoint;
		return fetch(endpoint, {
			body: JSON.stringify(userAudiences),
			headers: new Headers({
				Authorization: 'Bearer ' + settings.aditToken,
				'Content-Type': 'application/json'
			}),
			method: 'post'
		}).catch(function(error) {
			console.log('Error calling MS:' + error.message);
		});
	} catch (error) {
		// Retry on connection error
		console.log('Error calling MS:' + error.message);
		throw new RetryError(error.message);
	}
}
