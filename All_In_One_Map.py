'''
Produces a choropleth map with a drop down menu that will allow you to toggle between the included fields.
Script will need two files from the census bureau to accomplish that.

The tigerline shapefile for the block groups encompassing at least the specified geographic area (tl_2019_25_bg.zip) which can be obtained from https://www.census.gov/cgi-bin/geo/shapefiles/index.php
The estimated population table for the block groups encompassing the specified geographic area (2019 acs pop estimate bg.csv) which can be obtained from https://data.census.gov/cedsci/
'''

import geopandas as gpd
import plotly.express as px
import plotly.io as pio
import pandas as pd
import numpy as np
import plotly.graph_objs as go
import json
import psycopg2
import configparser
import time
from datetime import date


def runquery(tracts):

    '''
    function takes a list of census tract ids as a means of filtering the given sql query
    Query returns a number of stats grouped by the block group of all patrons within the specified tracts
    The key stat in the output, which will be used for the choropleth map later is the total_patron value.
    Other values are simply additional field that will be included in the hover tooltip within the final map.
    Query results are output as as pandas dataframe.
    '''
    
    # import configuration file containing our connection string
    # app.ini looks like the following
    #[db]
    #connection_string = dbname='iii' user='PUT_USERNAME_HERE' host='sierra-db.library-name.org' password='PUT_PASSWORD_HERE' port=ENTER_PORT_NUMBER_HERE

    config = configparser.ConfigParser()
    config.read('app_info.ini')
    
    query = """\
            SELECT
            CASE 
	            WHEN v.field_content IS NULL THEN 'no data' 
	            WHEN v.field_content = '' THEN v.field_content 
	            ELSE SUBSTRING(REGEXP_REPLACE(v.field_content,'\|(s|c|t|b)','','g'),1,12) 
            END AS geoid, 
            COUNT(DISTINCT p.id) AS total_patrons,
            SUM(p.checkout_total) AS total_checkouts,
            SUM(p.renewal_total) AS total_renewals,
            SUM(p.checkout_total + p.renewal_total) AS total_circ,
            SUM(p.checkout_count) AS total_checkouts_current,
            COUNT(DISTINCT h.id) AS total_holds_current,
            ROUND(AVG(DATE_PART('year',AGE(CURRENT_DATE,p.birth_date_gmt::DATE)))) AS avg_age,
            COUNT(DISTINCT p.id) FILTER(WHERE rm.creation_date_gmt::DATE >= NOW()::DATE - INTERVAL '1 year') AS total_new_patrons,
            COUNT(DISTINCT p.id) FILTER(WHERE p.activity_gmt::DATE >= NOW()::DATE - INTERVAL '1 year') AS total_active_patrons,
            ROUND(100.0 * (CAST(COUNT(DISTINCT p.id) FILTER(WHERE p.activity_gmt::DATE >= NOW()::DATE - INTERVAL '1 year') AS NUMERIC (12,2))) / CAST(COUNT(DISTINCT p.id) AS NUMERIC (12,2)), 2)::VARCHAR AS pct_active,
            COUNT(DISTINCT p.id) FILTER(WHERE ((p.mblock_code != '-') OR (p.owed_amt >= 10))) as total_blocked_patrons,
            ROUND(100.0 * (CAST(COUNT(DISTINCT p.id) FILTER(WHERE ((p.mblock_code != '-') OR (p.owed_amt >= 10))) as numeric (12,2)) / cast(COUNT(DISTINCT p.id) as numeric (12,2))),2)::VARCHAR AS pct_blocked,
            ROUND((100.0 * SUM(p.checkout_total))/(100.0 *COUNT(DISTINCT p.id)),2)::VARCHAR AS checkouts_per_patron,
            CASE
	            WHEN v.field_content IS NULL OR v.field_content = '' THEN 'na'
	            ELSE 'https://censusreporter.org/profiles/15000US'||SUBSTRING(REGEXP_REPLACE(v.field_content,'\|(s|c|t|b)','','g'),1,12)
            END AS census_reporter_url 
            FROM sierra_view.patron_record p 
            JOIN sierra_view.patron_record_address a 
            ON p.id = a.patron_record_id AND a.patron_record_address_type_id = '1' 
            JOIN sierra_view.record_metadata rm 
            ON p.id = rm.id 
            LEFT JOIN sierra_view.hold h 
            ON p.id = h.patron_record_id 
            LEFT JOIN sierra_view.varfield v 
            ON v.record_id = p.id AND v.varfield_type_code = 'k' AND v.field_content ~ '^\|s\d{2}' 
            WHERE SUBSTRING(REGEXP_REPLACE(v.field_content,'\|(s|c|t|b)','','g'),6,6) IN ("""\
            +tracts+"""\
            ) 
            GROUP BY 1,15 
            --HAVING COUNT(DISTINCT p.id) >= 80 
            ORDER BY 2 DESC
            """
      
    try:
	    # variable connection string should be defined in the imported config file
        conn = psycopg2.connect( config['db']['connection_string'] )
    except:
        print("unable to connect to the database")
        clear_connection()
        return
        
    #Opening a session and querying the database for weekly new items
    cursor = conn.cursor()
    cursor.execute(query)
    #For now, just storing the data in a variable. We'll use it later.
    rows = cursor.fetchall()
    conn.close()
    
    #convert query results to Pandas data frame
    column_names = ["geoid", "total_patrons", "total_checkouts","total_renewals","total_circ","total_checkouts_current","total_holds_current","avg_age","total_new_patrons","total_active_patrons","pct_active","total_blocked_patrons","pct_blocked","checkouts_per_patron","census_reporter_url"]
    df = pd.DataFrame(rows, columns=column_names)
    
    return df


def gen_map(library,patron_df):
    '''
    function produces plotly choropleth map based on query results from the runquery function
    
    Pandas is used to merge the queryresults data frame with the tigerline shape file and estimated population table from the Census Bureau
    Completed map is exported as a .html file
    '''
        
    zipfile = "zip://tl_2019_25_bg.zip"
    df = gpd.read_file(zipfile).to_crs("EPSG:4326")
    df.columns= df.columns.str.lower()
     
    #newdf = df.merge(patron_df, on='geoid', how='inner')  
    df = df.merge(patron_df, on='geoid', how='inner')
    
    pop_df = pd.read_csv("/Patron Maps/Data Sources/2019 acs pop estimate bg.csv", dtype={'geoid':str})
    
    df = df.merge(pop_df, on='geoid', how='inner')
    
    df['pct_cardholders'] = df.total_patrons / df.estimated_population * 100.00
    df['pct_cardholders'] = df['pct_cardholders'].round(decimals=2)

    zipjson = json.loads(df.to_json())
    
    #create list of fields that will be selectable in the map's drop down menu
    cols_dd = ['total_patrons', 'estimated_population', 'pct_cardholders', 'total_checkouts', 'checkouts_per_patron', 'total_new_patrons', 'total_active_patrons', 'pct_active', 'total_blocked_patrons','pct_blocked']

    visible = np.array(cols_dd)

    traces = []
    buttons = []
    
    #Create map for each field included in the cols_dd list
    '''
    z field will determine the value used for shading each region of the map.
    Full list of colorscale options available here https://plotly.com/python/builtin-colorscales/
    hovertemplate used to change the fields and labels from the dataframe that will display in the hover tooltip box over each region of the map
    '''
    for value in cols_dd:
        traces.append(go.Choropleth(
            locations = df.index,
            geojson = zipjson,
            z=df[value],
            colorbar_title=value,
            colorscale='YlGnBu',
            hovertemplate="<b>" + df.geographic_area_name + "</b><br>" +
                          "Total: " + df[value].astype(str) + "<extra></extra>",
            visible = True if value==cols_dd[0] else False
        ))

        buttons.append(dict(label=value, method='update', args=[{'visible':list(visible==value)},{'title':f'<b>{value}</b>'}]))

    updatemenus = [{'active':0, 'buttons':buttons}]

    fig = go.Figure(data=traces, layout=dict(updatemenus=updatemenus))

    first_title = cols_dd[0]
    fig.update_layout(title=f'<b>{first_title}</b>', title_x=0.5, geo = dict(scope='usa'))

    fig.update_geos(fitbounds="locations", visible=True)

    #Write resulting map to html file
    pio.write_html(fig, file=library+'AllInOneMap{}.html'.format(date.today()), auto_open=False)

def main(library,tracts):
    query_results = runquery(tracts)
    gen_map(library,query_results)

'''
Run for any number of location by calling the main function with the required variables
City/Town used for file naming
list of census tracts in that town to filter query on
'''

#main('Acton',"'363102','363103','363104','363201','363202'")
#main('Arlington',"'356100','356200','356300','356400','356500','356601','356602','356701','356702','356703','356704'")
#main('Ashland',"'385100','385201','385202'")
#main('Bedford',"'359100','359300'")
#main('Belmont',"'357100','357200','357300','357400','357500','357600','357700','357800'")
#main('Brookline',"'400100','400200','400201','400300','400400','400500','400600','400700','400800','400900','401000','401100','401200'")
#main('Cambridge',"'352101','352102','352200','352300','352400','352500','352600','352700','352800','352900','353000','353101','353102','353200','353300','353400','353500','353600','353700','353800','353900','354000','354100','354200','354300','354400','354500','354600','354700','354800','354900','355000'")
#main('Concord',"'361100','361200','361300'")   
#main('Dedham',"'402101','402102','402200','402300','402400','402500'")
#main('Dover',"'405100'")              
#main('Framingham Public',"'383101','383102','383200','383300','383400','383501','383502','383600','383700','383800','383901','383902','384001','384002'")
#main('Franklin',"'442101','442102','442103','442201','442202'")
#main('Holliston',"'387100','387201','387202'")
#main('Lexington',"'358100','358200','358300','358400','358500','358600','358700'")                            
#main('Lincoln',"'360100','360200','360300'")  
#main('Maynard',"'364101','364102'")
#main('Medfield',"'406101','406102'")
#main('Medford',"'339100','339200','339300','339400','339500','339600','339700','339801','339802','339900','340000','340100'")
#main('Medway',"'408101','408102'") 
#main('Millis',"'407100'")
#main('Natick',"'382100','382200','382300','382400','382500','382601','382602'")  
#main('Needham',"'403100','403300','403400','403500','457200'")
#main('Newton',"'373100','373200','373300','373400','373500','373600','373700','373800','373900','374000','374100','374200','374300','374400','374500','374600','374700','374800'")                      
#main('Norwood',"'413100','413200','413300','413401','413402','413500'")
#main('Sherborn',"'386100'")
#main('Somerville',"'350103','350104','350200','350300','350400','350500','350600','350700','350800','350900','351000','351100','351203','351204','351300','351403','351404','351500'")
#main('Stow',"'323100','980000'")
#main('Sudbury',"'365100','365201','365202'")
#main('Wayland',"'366100','366201','366202'")
#main('Waltham',"'368101','368102','368200','368300','368400','368500','368600','368700','368800','368901','368902','369000','369100'")
#main('Watertown',"'370101','370102','370104','370201','370202','370300','370301','370400','370401'")
#main('Wellesley',"'404100','404201','404202','404301','404302','404400'")
#main('Weston',"'367100','367200'")
#main('Westwood',"'412100','412200','412300'")  
#main('Winchester',"'338100','338200','338300','338400','338500'")
#main('Woburn',"'333100','333200','333300','333400','333501','333502','333600'") 
