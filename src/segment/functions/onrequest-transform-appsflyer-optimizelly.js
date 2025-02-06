async function onRequest(request, settings) {
	const body = request.json();

	trackToOptimizely(body);
	trackToAppsflyer(body);
}

function trackToOptimizely(body) {
	// Only track events are supported
	if (body.type !== 'track') return;

	// workaround for Int/Double mismatch in Segment.track modulea:
	// @error Failed calling Tracking API: json: cannot unmarshal number X.YZ into Go struct field ScreenInfo.context.screen.density of
	// type int
	if ('density' in body.context.screen) {
		body.context.screen.density = Math.round(body.context.screen.density);
	}

	// workaround for Int/String mismatch in Segment.track module
	// @error cannot unmarshal number into Go struct field AppInfo.context.app.build of type string
	if ('build' in body.context.app && Number.isInteger(body.context.app.build)) {
		body.context.app.build = body.context.app.build.toString();
	}

	if (body.event === 'Lead Triggered') {
		sendEventToOptimizely(body.properties.lead_type, body);
	}

	if (
		body.event === 'Detail Map Clicked' ||
		body.event === 'Detail Website Clicked' ||
		body.event === 'Detail Dealer Stock Clicked'
	) {
		sendEventToOptimizely(body.properties.hit_origin, body);
	}

	if (body.event === 'Ad Insertion Modal Viewed') {
		const type = body.properties.hit_information;
		if (
			type === 'recomendador precio error falta info' ||
			type === 'recomendador precio error red' ||
			type === 'recomendador precio error modelo'
		) {
			sendEventToOptimizely('Price Recommendation Error', body);
		} else if (
			type === 'precio recomendado o mantener' ||
			type === 'precio recomendado o introducir'
		) {
			sendEventToOptimizely('Price Recommendation Success', body);
		}
	}

	if (body.event === 'Filter Added') {
		const type = body.properties.last_filter;
		if (type === 'door_min' || type === 'door_max') {
			sendEventToOptimizely('Filter Added Door', body);
		}
	}

	if (body.event === 'Filter Added') {
		const type = body.properties.last_filter;
		if (type === 'seat_min' || type === 'seat_max') {
			sendEventToOptimizely('Filter Added Seat', body);
		}
	}

	if (body.event === 'Filter Added') {
		const type = body.properties.last_filter;
		if (type === 'door_min' || type === 'door_max') {
			sendEventToOptimizely('Filter Added Door', body);
		}
	}

	if (body.event === 'Filter Added') {
		const type = body.properties.last_filter;
		if (type === 'seat_min' || type === 'seat_max') {
			sendEventToOptimizely('Filter Added Seat', body);
		}
	}

	if (body.event === 'List Viewed') {
		const type = body.properties.list_type;
		if (type === 'popup anuncios recomendados') {
			sendEventToOptimizely('List Popup anuncios recomendados viewed', body);
		}
	}

	if (body.event === 'Ad Inserted') {
		const hit = body.properties.hit_information;
		if (hit === 'comparte en MA') {
			sendEventToOptimizely('Ad Inserted Shared MA', body);
		}
	}

	if (body.event === 'Ad Favorited') {
		sendEventToOptimizely(body.properties.hit_origin, body);
	}
}

function sendEventToOptimizely(name, body) {
	Segment.track({
		event: name,
		anonymousId: body.anonymousId,
		userId: body.userId,
		context: body.context,
		properties: body.properties,
		integrations: {
			Optimizely: true,
			AppsFlyer: false
		}
	});
}

function trackToAppsflyer(body) {
	// Ignore identify events.
	if (body.type === 'identify') return;
	if (!('appsFlyerId' in body.context.traits)) return;

	// Known properties
	// @see https://support.appsflyer.com/hc/en-us/articles/115005544169-AppsFlyer-Rich-In-App-Events-Android-and-iOS#event-types
	var mapping = {};
	mapping['ad_id'] = 'af_content_id';

	for (var key in mapping) {
		if (key in body.properties) {
			body.properties[mapping[key]] = body.properties[key];
		}
	}

	// Segment only forwards Application Opened events if they contain some properties such as installation date,
	// which are not available by default in their SDK. Hence we create a custom App Opened event.
	if (body.event === 'Application Opened') {
		body.event = 'App Opened';
	}

	if (
		body.event === 'Lead Triggered' &&
		body.properties.lead_type === 'Lead to OEM'
	) {
		sendEventToAppsFlyer('Lead Triggered Vehiculo Nuevo', body);
	}

	if (!('advertisingId' in body.context.device)) {
		body.context.device.advertisingId = '00000000-0000-0000-0000-000000000000';
	}

	// workaround for Int/Double mismatch in Segment.track module:
	// @error Failed calling Tracking API: json: cannot unmarshal number X.YZ into Go struct field ScreenInfo.context.screen.density of type int
	if ('density' in body.context.screen) {
		body.context.screen.density = Math.round(body.context.screen.density);
	}

	// workaround for Int/String mismatch in Segment.track module
	// @error cannot unmarshal number into Go struct field AppInfo.context.app.build of type string
	if ('build' in body.context.app && Number.isInteger(body.context.app.build)) {
		body.context.app.build = body.context.app.build.toString();
	}

	sendEventToAppsFlyer(body.event, body);
}

function sendEventToAppsFlyer(name, body) {
	Segment.track({
		event: name,
		anonymousId: body.anonymousId,
		userId: body.userId,
		context: body.context,
		properties: body.properties,
		integrations: {
			AppsFlyer: {
				appsFlyerId: body.context.traits.appsFlyerId
			},
			Optimizely: false
		}
	});
}
