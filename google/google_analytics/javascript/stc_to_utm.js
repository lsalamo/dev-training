let paramsString = window.location.search;
let searchParams = new URLSearchParams(paramsString);
if (searchParams.has("stc") === true) {
    stc = searchParams.get("stc");
    var res = stc.split("-");
    medium = res[0];
    source = res[1];
    campaign_name = res[2];
    content = res[3];
    keywword = res[4];
}
analytics.track("Home Viewed", {
    page_name: "homepage"
}, {
campaign: {
    source: source,
    medium: medium,
    name: campaign_name,
    content: content,
    keyword: keyword
    }
});
