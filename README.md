# Irish Spatial Data Exchange Monitoring

A Python script to generate Pandas DataFrames for use in a dashboard
to aid transparency on:

- The health of the Irish Spatial Data Exchange (ISDE) network
- The latest metadata in the nodes of the ISDE network
- Structural validity of the XML records in the ISDE network

## Requirements

- dash >= 2.3.1
  - _Used for dashboard front-end generation_
- pandas >= 1.4.2 
  - _Used to generate DataFrames for use in dashboards, for example PowerBI or Dash_
- xmlschema >= 1.10.0
  - _Used to validate the individual metadata records_