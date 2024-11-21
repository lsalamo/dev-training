window.PulseAdapter.getListFilters = function() {
    return {
        "query": (b.search_terms || ""),
        "region": (b.region_level2 || ""),
        "adType": "Sell",
        "minYear": (b.year_min || ""),
        "maxYear": (b.year_max || ""),
        "minPrice": (b.price_min || ""),
        "maxPrice": (b.price_max || ""),
        "minMileage": (b.km_min || ""),
        "maxMileage": (b.km_max || ""),
        "fuelType": (b.fuel || ""),
        "transmission": (b.transmission || "")
    };
};
window.PulseAdapter.getFavouriteIntent = function(e) {
    var result = "";
    var intentIndexStr = (String(e.intentIndex) || "");
    if (intentIndexStr !== "") {
        result = e.intentIndex;
    } else {
        if (e.page === "list") {
            result = (event.currentTarget.className.indexOf("0_xs") >= 0 ? window.PulseAdapter.intentIndex.save : window.PulseAdapter.intentIndex.unsave);
        } else if (e.page === "detail") {
            result = ((event.currentTarget.querySelector(".js-favoriteIcon").getAttribute("xlink:href") || "").indexOf("star-full") >= 0 ? window.PulseAdapter.intentIndex.unsave : window.PulseAdapter.intentIndex.save);
        }
    }
    return (result === window.PulseAdapter.intentIndex.save ? "Save" : "Unsave");
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

    var eventEngagementAdFromListing = function(item, itemId, itemTitle) {
        document.querySelectorAll(item).forEach(function(e) {
            var id = ((e.querySelector(itemId) || "").id || "");
            var title = ((e.querySelector(itemTitle) || "").innerText || "");
            eventEngagementExecute("[data-tagging='c_list_detail_saved_ad']", window.PulseAdapter.eventFavourite, id, title, "", typeIndex.ad, e);
        });
    };

    var eventEngagementList = function(item, callback, intentIndex) {
        var id = (b["dom.url"] || "");
        var title = (b["dom.title"] || "");
        eventEngagementExecute(item, callback, id, title, intentIndex, typeIndex.listing, null);
    };

    var eventEngagementAlerts = function(item, callback, intentIndex) {
        eventEngagementExecute(item, callback, "eventEngagementAlerts", "", intentIndex, typeIndex.listing, null);
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
                e.addEventListener("click", function(){
                    callback(data);
                });
            }
        });
    };

    if (page === "list") {
        /***
        * Event: Save favourite
        ***/
        eventEngagementAdFromListing("article.mt-Card", "[data-tagging='c_list_detail_saved_ad']", ".mt-CardAd-title .mt-CardAd-titleHiglight");

        /***
        * Event: Save Listing
        ***/
        eventEngagementList("#btnSaveAlert", window.PulseAdapter.eventFavourite, window.PulseAdapter.intentIndex.save);
    } else if (page === "detail") {
        /***
        * Event: Save favourite
        ***/
        eventEngagementAd(".js-checkFavorite", window.PulseAdapter.eventFavourite);

        /***
        * Event: Show phone
        * keywords:
        *   c_detail_show_phone_professional
        *   c_detail_call_phone
        *   c_detail_call_phone_form_detail
        ***/
        eventEngagementAd("[data-tagging*='phone']", window.PulseAdapter.eventPhone);

        /***
        * Event: Share ClassifiedAd
        * keywords:
        *   c_detail_share_ad_email
        *   c_detail_share_ad_facebook
        *   c_detail_share_ad_twitter
        *   c_detail_share_ad_googleplus
        *   c_detail_share_ad
        ***/
        eventEngagementAd("[data-tagging*='share']", window.PulseAdapter.eventShare);

        /***
        * Event: View Form - Reply
        ***/
        eventEngagementAd("#js-contact-scroll", window.PulseAdapter.eventViewFormReply);
    } else if (page === "my_alerts") {
        /***
        * Event: Unsave Listing
        ***/
        eventEngagementAlerts("#linkDeleteAlert", window.PulseAdapter.eventFavourite, window.PulseAdapter.intentIndex.unsave);
    }
})();
