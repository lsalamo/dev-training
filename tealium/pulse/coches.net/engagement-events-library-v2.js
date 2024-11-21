(function() {

    window.PulseAdapter = (window.PulseAdapter || {});

    window.PulseAdapter.isLoad = function() {
        return window.PulseAdapter.load || false;
    };

    var init = function() {
        window.PulseAdapter.eventClick = function(item, callback) {
            document.querySelectorAll(item).forEach(function(e) {
                e.addEventListener("click", function(){
                    callback();
                }, true);
            });
        };

        var sendPulseEvent = function(data) {
            pulse('update', data);
            pulse('track');
        };

        var getObjectClassified = function() {
            var id = (window.PulseAdapter.data.spt_ad_id || "");
            var pageId = (window.PulseAdapter.page_type === "detail" ? "classified" : "listing");
            var pageType = (window.PulseAdapter.page_type === "detail" ? "ClassifiedAd" : "Listing");
            return {
                "@id": "sdrn:" + window.PulseAdapter.site + ":" + pageId + ":" + id,
                "@type": pageType,
                "category": PulseAdapter.data.spt_format_category,
                "name": PulseAdapter.data.spt_name
            };
        };

        var setFavourite = function() {
            var eventFavourite = function(type) {
                type = (type || "");
                var data = {};
                data.type = (type || "");
                //data.object = getObjectClassified();
                if (data.type !== "") {
                    data.type = type;
                } else if (typeof event !== "undefined") {
                    switch (window.PulseAdapter.site) {
                        case "fotocasaes":
                        data.type = (event.currentTarget.className.indexOf("active") >= 0 ? "Unsave" : "Save");
                        break;
                        default:
                        data.type = "";
                    }
                }
                if (data.type !== "") {
                    sendPulseEvent(data);
                }
            };

            window.PulseAdapter.eventFavouriteAd = function() {
                eventFavourite();
            };

            window.PulseAdapter.eventFavouriteListSave = function() {
                eventFavourite("Save");
            };
        };

        var setPhone = function() {
            var eventPhone = function(type) {
                var id = (window.PulseAdapter.data.spt_ad_id || "");
                var data = {};
                data.object = {
                    "@id": "sdrn:" + window.PulseAdapter.site + ":phonecontact:" + id,
                    "@type": "PhoneContact",
                    "name": "Ad phone number displayed"
                };
                data.object.inReplyTo = getObjectClassified();
                data.type = type;
                sendPulseEvent(data);
            };

            window.PulseAdapter.eventShowPhone = function() {
                eventPhone("Show");
            };

            window.PulseAdapter.eventCallPhone = function() {
                eventPhone("Call");
            };
        };

        var setShare = function() {
            window.PulseAdapter.eventShare = function() {
                var data = {};
                data.object = getObjectClassified();
                data.type = "Share";
                sendPulseEvent(data);
            };
        };

        window.PulseAdapter.site = (b.spt_client_id || "");
        window.PulseAdapter.page_type = (b.pl_page || b.event_name || "").toLowerCase();
        window.PulseAdapter.data = (b || "");
        window.PulseAdapter.load = false; //Semaforo para evitar que se ejecute otra vez sobre fake page views

        setFavourite();

        setPhone();

        setShare();
    };

    if (!window.PulseAdapter.isLoad()) {
        init();
    }

})();
