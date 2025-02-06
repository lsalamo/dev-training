Adobe Analytics API v2.0 with DMP dimensions
--------------------------------------------

This is a python wrapper for the adobe analytics API 2.0.

## Documentation

Most of the documentation for this API will be hosted at: [Adobe Github][1].

## Functionality
Basic Functionality that are covered :
- Run a report statement  
- Retrieve Analytics Dimensions
- Retrieve DMP Dimensions (objective)

# Getting Started

To install the library with PIP use:

```bash
python -m pip install --upgrade git+https://github.com/pitchmuc/adobe_analytics_api_2.0.git#egg=adobe_analytics_2
```

## Dependencies

In order to use this API in python, you would need to have those libraries installed :
- pandas
- requests
- json
- PyJWT
- PyJWT[crypto]
- pathlib

## Others Sources

You can find information about the Adobe Analytics API 2.0 here :
- [https://adobedocs.github.io/analytics-2.0-apis][2]
- [https://github.com/AdobeDocs/analytics-2.0-apis/blob/master/reporting-guide.md][3]

[1]: https://github.com/AdobeDocs/analytics-2.0-apis
[2]: https://adobedocs.github.io/analytics-2.0-apis
[3]: https://github.com/AdobeDocs/analytics-2.0-apis/blob/master/reporting-guide.md
