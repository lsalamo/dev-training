(function() {
    var page = (window.PulseAdapter.page_type || "");
    if (!window.PulseAdapter.isLoad()) {
        window.PulseAdapter.load = true;
        if (page === "list") {
            window.PulseAdapter.eventClick(".fc-Save-search button", window.PulseAdapter.eventFavouriteListSave);
        } else if (page === "detail") {
            window.PulseAdapter.eventClick("#liFavorite", window.PulseAdapter.eventFavouriteAd);
            window.PulseAdapter.eventClick("#ctl00_buttonContextualShowPhone a", window.PulseAdapter.eventShowPhone);
            window.PulseAdapter.eventClick("#ctl00_lnkShared", window.PulseAdapter.eventShare);
        }
    }
})();
