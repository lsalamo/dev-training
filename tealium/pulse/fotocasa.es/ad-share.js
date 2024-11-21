$("#ctl00_lnkShared").click(function() {
    var spt_intent = 'Share';
    var spt_ad_id = utag_data.ad_id;
    var spt_interaction = 'Click';
    var spt_item_identifier = '#ctl00_lnkShared';
    var spt_item_type = 'UIElement';
    var spt_site = 'fotocasaes';
    var spt_target = {
        item: {
            "@id": "sdrn:" + spt_site + ":classified:" + spt_ad_id,
            "@type": "ClassifiedAd",
            "category": utag_data.spt_format_category
        }
    };
    AutoTrack.activity().events.trackEngagement(0, 0, spt_interaction, spt_item_type, spt_item_identifier, spt_intent, spt_target).send();
    AutoTrack.activity().events.trackBasic(spt_intent, 'ClassifiedAd', spt_ad_id, null).send();
});
