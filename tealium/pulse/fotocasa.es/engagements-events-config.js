window.PulseAdapter.getFavouriteIntent = function(e) {
    var result = "";
    var intentIndexStr = (String(e.intentIndex) || "");
    if (intentIndexStr !== "") {
        result = e.intentIndex;
    } else {
        if (e.page === "list") {
            var element = event.currentTarget.firstElementChild;
            result = (element && element.className.indexOf("icon-favourite-fill") >= 0 ? window.PulseAdapter.intentIndex.unsave : window.PulseAdapter.intentIndex.save);
        } else if (e.page === "detail") {
            result = (event.currentTarget.className.indexOf("active") >= 0 ? window.PulseAdapter.intentIndex.unsave : window.PulseAdapter.intentIndex.save);
        }
    }
    return (result === window.PulseAdapter.intentIndex.save ? "Save" : "Unsave");
};

window.PulseAdapter.eventViewFormReply = function(e) {
    utag_data.event_engagement = "adreply_form";
    utag.view(utag_data, null, [25]);
};

(function() {
    var item = null;
    var itemId = "";
    var page = (b.pl_page || b.event_name || "").toLowerCase();
    var typeIndex = { "ad" : 0, "listing" : 1};

    var eventEngagementAd = function(item, callback) {
        var id = (b.ad_id || "");
        var title = (b.ad_title || "");
        eventEngagementExecute(item, callback, id, title, "", typeIndex.ad, null);
    };

    var eventEngagementAdFromListing = function() {
        document.querySelectorAll(".aditem").forEach(function(e) {
            var id = ((e.querySelector(".aditem-footer") || "").id || "").replace("ph", "");
            var title = ((e.querySelector("a.aditem-detail-title") || "").innerText || "");
            eventEngagementExecute("a i[class*='share']", window.PulseAdapter.eventShare, id, title, "", typeIndex.ad, e);
            eventEngagementExecute("a.highlighted-button", window.PulseAdapter.eventPhone, id, title, "", typeIndex.ad, e);
            eventEngagementExecute("a i[class*='favourite']", window.PulseAdapter.eventFavourite, id, title, "", typeIndex.ad, e);
        });
    };

    var eventEngagementList = function(item, callback, intentIndex) {
        var id = (b["dom.url"] || "");
        var title = (b["dom.title"] || "");
        eventEngagementExecute(item, callback, id, title, intentIndex, typeIndex.listing, null);
    };

    var eventEngagementExecute = function(item, callback, id, title, intentIndex, typeIndex, e) {
        var isEventEngagementAlerts = (id === "eventEngagementAlerts");
        (e || document).querySelectorAll(item).forEach(function(e) {
            if (isEventEngagementAlerts) {
                id = ((e.parentNode.querySelector("a#linkSearch") || "").href || "");
                title = (b["dom.title"] || "");
            }
            if (id !== "") {
                var data = {"id" : id, "title" : title, "typeIndex" : typeIndex, "page" : page, "item" : item, "intentIndex" : intentIndex};
                //e = (callback === window.PulseAdapter.eventShare || (window.PulseAdapter.eventFavourite && page === "list") ? e.parentNode : e);
                e.addEventListener("click", function(){
                    callback(data);
                }, true);
            }
        });
    };

    if (page === "list") {
        //eventEngagementAdFromListing();
        eventEngagementList(".fc-Save-search button", window.PulseAdapter.eventFavourite, window.PulseAdapter.intentIndex.save);
    } else if (page === "detail") {
        eventEngagementAd("#liFavorite", window.PulseAdapter.eventFavourite);
        eventEngagementAd("#ctl00_buttonContextualShowPhone a", window.PulseAdapter.eventPhone);
        eventEngagementAd("#ctl00_lnkShared", window.PulseAdapter.eventShare);
        eventEngagementAd("a.header-pagination_item_inner[data-contact-focus]", window.PulseAdapter.eventViewFormReply);
    }
})();
