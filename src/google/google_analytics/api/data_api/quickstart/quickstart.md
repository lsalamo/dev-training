# Connecting to a Restricted Google Sheet with Python

## Docs

- [Google Analytics](https://developers.google.com/analytics)
- [GitHub](https://github.com/googleapis/google-cloud-python/tree/main/packages/google-analytics-data)
- [GitHub Snippets]()
- [API Reference](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/)

## Quickstart

1. Set Up Google Cloud Platform (GCP) Project:

- Create a new [GCP](https://console.cloud.google.com/) project.
- Enable the [Google Analytics Data API](https://console.cloud.google.com/apis/api/analyticsdata.googleapis.com).
```
> APIs and services > Enabled APIs and Services > Google Analytics Data API > Enable
```

2. Crear una cuenta de servicio

```
> APIs and services > Credentials > Service Accounts > Create Service Account
```

3. En la cuenta de servicio creada agregar nueva clave

```
> APIs and services > Credentials > Service Accounts > Create Service Account > [Select Service Account] > Keys > Add Keys > JSON > Download JSON
```

4. Conceder permisos

- Navegar a [Google Marketing Platform (GMP)](https://marketingplatform.google.com/).

```
> GMP > Administration > [Select Organisation] > Analytics accounts > [Select Analytic account] > Account Users > Add new user "client_email" from the JSON Key file (google-analytics-data-ga4-api@api-project-329785109876.iam.gserviceaccount.com) > [Grant it "Analyts" access to your analytics property]
```

- Navegar a [Google Analytics](https://analytics.google.com/).

```
> GA > Admin > Account Settings > Account Access Management > Add new user "client_email" from the JSON Key file (google-analytics-data-ga4-api@api-project-329785109876.iam.gserviceaccount.com) > [Grant it "Analyts" access to your analytics property]
```
