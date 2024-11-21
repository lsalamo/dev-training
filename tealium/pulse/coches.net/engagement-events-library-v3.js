/*
    pulse('init', 'fotocasaes', null, null, 'tracker2');
    pulse('trackPageView');
    pulse('tracker2.trackPageView');
    pulse('tracker2.track');

    var data = {};
    data.object = {"@id" : "sdrn:fotocasaes:classified:1234", "@type" : "ClassifiedAd", "category": PulseAdapter.data.spt_format_category};
    pulse('tracker2.track', 'trackerEvent', data);
*/
(function() {

    window.PulseAdapter = (window.PulseAdapter || {});
    window.PulseAdapter.site = (b.spt_client_id || "");
    window.PulseAdapter.category = (b.spt_format_category || "");

    var getObjectClassified = function(id, name) {
        var data = {};
        id = (id || "");
        name = (name || "");
        if (id !== "") {
            data = {
                "@id": "sdrn:" + window.PulseAdapter.site + ":classified:" + id,
                "@type": "ClassifiedAd",
                "category": window.PulseAdapter.category,
                "name": name
            };
        }
        return data;
    };

    var sendPulseEvent = function(data) {
        pulse('init', window.PulseAdapter.site, null, null, 'trackerAction');
        pulse('trackerAction.track', 'trackerEvent', data);
    };

    window.PulseAdapter.eventFavouriteAdUnsave = function(id, name) {
        var data = {};
        data.type = "Unsave";
        data.object = getObjectClassified(id, name);
        sendPulseEvent(data);
    };

    window.PulseAdapter.eventFavouriteAdSave = function(id, name) {
        var data = {};
        data.type = "Save";
        data.object = getObjectClassified(id, name);
        sendPulseEvent(data);
    };

    window.PulseAdapter.eventFavouriteListSave = function() {
        var data = {};
        data.type = "Save";
        pulse('update', data);
        pulse('track');
    };

    window.PulseAdapter.eventShowPhone = function(id, name) {
        var data = {};
        data.type = "Show";
        data.object = {
            "@id": "sdrn:" + window.PulseAdapter.site + ":phonecontact:" + id,
            "@type": "PhoneContact",
            "name": "Ad phone number displayed"
        };
        data.object.inReplyTo = getObjectClassified(id, name);
        sendPulseEvent(data);
    };

    window.PulseAdapter.eventCallPhone = function(id, name) {
        var data = {};
        data.type = "Call";
        data.object = {
            "@id": "sdrn:" + window.PulseAdapter.site + ":phonecontact:" + id,
            "@type": "PhoneContact",
            "name": "Ad phone number displayed"
        };
        data.object.inReplyTo = getObjectClassified(id, name);
        sendPulseEvent(data);
    };

    window.PulseAdapter.eventShare = function(id, name) {
        var data = {};
        data.type = "Share";
        data.object = getObjectClassified(id, name);
        sendPulseEvent(data);
    };

    window.PulseAdapter.eventForm = function(id, name, intent) {
        var data = {};
        data.type = "View";
		data.object = {};
		data.object["@id"] = "sdrn:" + window.PulseAdapter.site + ":form:" + utag_data["dom.url"];
		data.object["@type"] = "Form";
        data.object.name = name;
        data.object.url = utag_data["dom.url"];
		data.intent = intent;
        data.target = getObjectClassified(id, name);
        sendPulseEvent(data);
    };

    window.PulseAdapter.eventConfirmation = function(id, name, intent) {
        var data = {};
        data.type = "View";
		data.object = {};
		data.object["@id"] = "sdrn:" + window.PulseAdapter.site + ":confirmation:" + utag_data["dom.url"];
		data.object["@type"] = "Confirmation";
        data.object.name = name;
        data.object.url = utag_data["dom.url"];
		data.intent = intent;
        data.target = getObjectClassified(id, name);
        sendPulseEvent(data);
    };

    window.PulseAdapter.eventFormSend = function(id, name) {
        window.PulseAdapter.eventForm(id, name, "Send");
    };

    window.PulseAdapter.eventConfirmationSend = function(id, name) {
        window.PulseAdapter.eventConfirmation(id, name, "Send");
    };

    window.PulseAdapter.eventSendMessage = function(id, name) {
        var data = {};
        data.type = "Send";
		data.object = {};
		data.object["@id"] = "sdrn:" + window.PulseAdapter.site + ":message:" + id;
		data.object["@type"] = "Message";
        data.object.name = "Send Message";
        data.object.url = utag_data["dom.url"];
        data.object.inReplyTo = getObjectClassified(id, name);
        sendPulseEvent(data);
    };

    window.PulseAdapter.eventCreateClassified = function(id, name) {
        var data = {};
        data.type = "Create";
        data.object = getObjectClassified(id, name);
        sendPulseEvent(data);
    };

})();
