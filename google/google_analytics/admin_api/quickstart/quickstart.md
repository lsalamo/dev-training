# Connecting to a Restricted Google Sheet with Python

## Docs

- [Google Analytics](https://developers.google.com/analytics)
- [GitHub](https://github.com/googleapis/google-cloud-python/tree/main/packages/google-analytics-data)
- [GitHub Snippets]()
- [API Reference](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/)

## Quickstart

1. Set Up Google Cloud Platform (GCP) Project:

- Create a new [GCP](https://console.cloud.google.com/) project.
- Enable the [Google Analytics Admin API](https://console.cloud.google.com/apis/library/analyticsadmin.googleapis.com).
```
> APIs and services > Enabled APIs and Services > Google Analytics Admin API > Habilitar
```

2. Crear una cuenta de servicio

```
> APIs and services > Credentials > Service Accounts > Create Service Account
```

3. En la cuenta de servicio creada agregar nueva clave

```
> APIs and services > Credentials > Service Accounts > Create Service Account > [Select Service Account] > Keys > Add Keys > JSON > Download JSON
```

