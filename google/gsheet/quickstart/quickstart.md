# Connecting to a Restricted Google Sheet with Python

## Docs

- [Google Dev](https://developers.google.com/sheets/api/quickstart/python?hl=es-419)
- [GitHub](https://github.com/googleworkspace/python-samples/blob/main/sheets/quickstart/quickstart.py)
- [GitHub Snippets](https://github.com/googleworkspace/python-samples/tree/main/sheets/snippets)
- [API Reference](https://developers.google.com/sheets/api/reference/rest)
- [Scopes: Elige los permisos de la API de Hojas de cálculo de Google](https://developers.google.com/sheets/api/scopes)

## Quickstart

1. Set Up Google Cloud Platform (GCP) Project:

- Create a new [GCP](https://console.cloud.google.com/) project.
- Enable the [Google Sheets API](https://console.cloud.google.com/apis/library/sheets.googleapis.com).
```
> APIs and services > Enabled APIs and Services > Google Sheets API > Habilitar
```

2. Cómo configurar la pantalla de consentimiento de OAuth

- Enable the [Google Sheets API](https://console.cloud.google.com/apis/library/sheets.googleapis.com).
```
> APIs and services > OAuth consent screen > User Type (Internal) > Create > [fill app data] > Save and continue
```

Por ahora, puedes omitir el paso de agregar permisos y hacer clic en **Guardar y continuar**. En el futuro, cuando crees una app para usarla fuera de tu organización de Google Workspace, debes cambiar el **Tipo de usuario a Externo** y, luego, agregar los permisos de autorización que requiere tu app.

3. Autoriza credenciales para una aplicación de escritorio

- Create a OAuth Client ID
```
> APIs and services > Credentials > Create Credentials > OAuth Client ID > [Type Application Type "Desktop App" and name "API Google Sheet Credential"] > Download JSON 
```
