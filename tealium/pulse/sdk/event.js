function configureEvent(originalDataObj, internalDataObj) {
    internalDataObj.contentType = '';
    if (typeof originalDataObj.spt_content_type === 'string' && originalDataObj.spt_content_type) {
        internalDataObj.contentType = originalDataObj.spt_content_type;
    } else if (originalDataObj.spt_is_adview === 'true') {
        internalDataObj.contentType = 'ClassifiedAd';
    } else if (originalDataObj.spt_is_frontpage === 'true') {
        internalDataObj.contentType = 'Frontpage';
    } else if (originalDataObj.spt_is_listing === 'true') {
        internalDataObj.contentType = 'Listing';
    } else if (originalDataObj.spt_is_adinsertion_form === 'true') {
        internalDataObj.contentType = 'Form';
        internalDataObj.targetType = 'ClassifiedAd';
        internalDataObj.targetId = '0';
        internalDataObj.intent = 'Create';
    } else if (originalDataObj.spt_is_adinsertion_error === 'true') {
        internalDataObj.contentType = 'Error';
        internalDataObj.targetType = 'ClassifiedAd';
        internalDataObj.targetId = '0';
        internalDataObj.intent = 'Create';
    } else if (originalDataObj.spt_is_adinsertion_confirmation === 'true') {
        internalDataObj.contentType = 'Confirmation';
        internalDataObj.targetType = 'ClassifiedAd';
        if (internalDataObj.adId) {
            internalDataObj.targetId = internalDataObj.adId;
        }
        internalDataObj.intent = 'Create';
    } else if (originalDataObj.spt_is_admodification_form === 'true') {
        internalDataObj.contentType = 'Form';
        internalDataObj.targetType = 'ClassifiedAd';
        if (internalDataObj.adId) {
            internalDataObj.targetId = internalDataObj.adId;
        }
        internalDataObj.intent = 'Update';
    } else if (originalDataObj.spt_is_admodification_error === 'true') {
        internalDataObj.contentType = 'Error';
        internalDataObj.targetType = 'ClassifiedAd';
        if (internalDataObj.adId) {
            internalDataObj.targetId = internalDataObj.adId;
        }
        internalDataObj.intent = 'Update';
    } else if (originalDataObj.spt_is_admodification_confirmation === 'true') {
        internalDataObj.contentType = 'Confirmation';
        internalDataObj.targetType = 'ClassifiedAd';
        if (internalDataObj.adId) {
            internalDataObj.targetId = internalDataObj.adId;
        }
        internalDataObj.intent = 'Update';
    } else if (originalDataObj.spt_is_adreport_form === 'true') {
        internalDataObj.contentType = 'Form';
        internalDataObj.targetType = 'ClassifiedAd';
        if (internalDataObj.adId) {
            internalDataObj.targetId = internalDataObj.adId;
        }
        internalDataObj.intent = 'Report';
    } else if (originalDataObj.spt_is_adreport_error === 'true') {
        internalDataObj.contentType = 'Error';
        internalDataObj.targetType = 'ClassifiedAd';
        if (internalDataObj.adId) {
            internalDataObj.targetId = internalDataObj.adId;
        }
        internalDataObj.intent = 'Report';
    } else if (originalDataObj.spt_is_adreport_confirmation === 'true') {
        internalDataObj.contentType = 'Confirmation';
        internalDataObj.targetType = 'ClassifiedAd';
        if (internalDataObj.adId) {
            internalDataObj.targetId = internalDataObj.adId;
        }
        internalDataObj.intent = 'Report';
    } else if (originalDataObj.spt_is_adrefer_form === 'true') {
        internalDataObj.contentType = 'Form';
        internalDataObj.targetType = 'ClassifiedAd';
        if (internalDataObj.adId) {
            internalDataObj.targetId = internalDataObj.adId;
        }
        internalDataObj.intent = 'Refer';
    } else if (originalDataObj.spt_is_adrefer_error === 'true') {
        internalDataObj.contentType = 'Error';
        internalDataObj.targetType = 'ClassifiedAd';
        if (internalDataObj.adId) {
            internalDataObj.targetId = internalDataObj.adId;
        }
        internalDataObj.intent = 'Refer';
    } else if (originalDataObj.spt_is_adrefer_confirmation === 'true') {
        internalDataObj.contentType = 'Confirmation';
        internalDataObj.targetType = 'ClassifiedAd';
        if (internalDataObj.adId) {
            internalDataObj.targetId = internalDataObj.adId;
        }
        internalDataObj.intent = 'Refer';
    } else if (originalDataObj.spt_is_registration_form === 'true') {
        internalDataObj.contentType = 'Form';
        internalDataObj.targetType = 'Account';
        internalDataObj.targetId = '0';
        internalDataObj.intent = 'Create';
    } else if (originalDataObj.spt_is_registration_error === 'true') {
        internalDataObj.contentType = 'Error';
        internalDataObj.targetType = 'Account';
        internalDataObj.targetId = '0';
        internalDataObj.intent = 'Create';
    } else if (originalDataObj.spt_is_registration_confirmation === 'true') {
        internalDataObj.contentType = 'Confirmation';
        internalDataObj.targetType = 'Account';
        if (internalDataObj.userId) {
            internalDataObj.targetId = internalDataObj.userId;
        }
        internalDataObj.intent = 'Create';
    } else if (originalDataObj.spt_is_login_form === 'true') {
        internalDataObj.contentType = 'Form';
        internalDataObj.targetType = 'Account';
        internalDataObj.targetId = '0';
        internalDataObj.intent = 'Login';
    } else if (originalDataObj.spt_is_login_error === 'true') {
        internalDataObj.contentType = 'Error';
        internalDataObj.targetType = 'Account';
        internalDataObj.targetId = '0';
        internalDataObj.intent = 'Login';
    } else if (originalDataObj.spt_is_login_confirmation === 'true') {
        internalDataObj.contentType = 'Confirmation';
        internalDataObj.targetType = 'Account';
        if (internalDataObj.userId) {
            internalDataObj.targetId = internalDataObj.userId;
        }
        internalDataObj.intent = 'Login';
    } else if (originalDataObj.spt_is_adreply_form === 'true') {
        internalDataObj.contentType = 'Form';
        internalDataObj.targetType = 'Message';
        internalDataObj.intent = 'Send';
        internalDataObj.inReplyTo = '{"@id": "' + internalDataObj.adId + '", "@type": "ClassifiedAd"}';
    } else if (originalDataObj.spt_is_adreply_error === 'true') {
        internalDataObj.contentType = 'Error';
        internalDataObj.targetType = 'Message';
        internalDataObj.intent = 'Send';
        internalDataObj.inReplyTo = '{"@id": "' + internalDataObj.adId + '", "@type": "ClassifiedAd"}';
    } else if (originalDataObj.spt_is_adreply_confirmation === 'true') {
        internalDataObj.contentType = 'Confirmation';
        internalDataObj.targetType = 'Message';
        internalDataObj.intent = 'Send';
        internalDataObj.inReplyTo = '{"@id": "' + internalDataObj.adId + '", "@type": "ClassifiedAd"}';
    } else if (originalDataObj.spt_is_premiumfeature_form === 'true') {
        internalDataObj.contentType = 'Form';
        internalDataObj.targetType = 'PremiumFeature';
        internalDataObj.targetId = '0';
    } else if (originalDataObj.spt_is_premiumfeature_error === 'true') {
        internalDataObj.contentType = 'Error';
        internalDataObj.targetType = 'PremiumFeature';
        internalDataObj.targetId = '0';
    } else if (originalDataObj.spt_is_premiumfeature_confirmation === 'true') {
        internalDataObj.contentType = 'Confirmation';
        internalDataObj.targetType = 'PremiumFeature';
        internalDataObj.targetId = '0';
    } else {}
    if (!internalDataObj.contentType) {
        internalDataObj.contentType = 'Content';
    }
}
