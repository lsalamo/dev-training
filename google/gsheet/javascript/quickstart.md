# Connecting to Google Sheet with Javascript

## Docs

- [Google Dev](https://developers.google.com/sheets/api/quickstart/js)
- [GitHub](https://github.com/googleworkspace/browser-samples/blob/main/sheets/quickstart/index.html)

## Quickstart

1. Set Up Google Cloud Platform (GCP) Project:

- Create a new [GCP](https://console.cloud.google.com/) project.
- Enable the [Google Sheets API](https://console.cloud.google.com/apis/library/sheets.googleapis.com).
```
> APIs and services > Enabled APIs and Services > Google Sheets API > Habilitar
```

2. Cómo configurar la pantalla de consentimiento de OAuth

```
> APIs and services > OAuth consent screen > User Type (Internal) > Create > [fill app data] > Save and continue
```

Por ahora, puedes omitir el paso de agregar permisos y hacer clic en **Guardar y continuar**. En el futuro, cuando crees una app para usarla fuera de tu organización de Google Workspace, debes cambiar el **Tipo de usuario a Externo** y, luego, agregar los permisos de autorización que requiere tu app.

3. Autoriza credenciales para una aplicación web

- Create OAuth Client ID
```
> APIs and services > Credentials > Create Credentials > OAuth Client ID > [Type Application Type "Web application" and name "API Google Sheet Credential"] > [Add Authorised JavaScript origins "http://localhost:8000"] > create
```

4. Crear una clave API

```
> APIs and services > Credentials > Create Credentials > API key > [Click "Restrict key" to update advanced settings and limit use of your API key]
```

4. Run

```bash
python3 -m http.server 8000
# And opening the web page:
open http://localhost:8000
open http://localhost:8000/Documents/github/python-training/google/gsheet/javascript/2_example.html
```
