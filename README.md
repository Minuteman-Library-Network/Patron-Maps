# Patron Maps

Python scripts to produce Choropleth maps based around data in our Sierra patron records and tables available from the U.S. Census Bureau.

Key to this script is the presence of a census field that we have added to our patron records using the Cenus Bureau Geocoder (https://geocoding.geo.census.gov/geocoder)
For records where an address has been successfully matched to the geocoder the census block number is added to the patron record.  This allows for our SQL scripts to group statistics at the state, county, tract or block group level for these maps.

Created and maintained by Jeremy Goldstein for test purposes only. Use at your own risk. Not supported by the Minuteman Library Network. 
