"""
Displays a monitoring dashboard to display the status of the nodes in the Irish
Spatial Data Exchange network.

:author: Adam Leadbetter (@adamml)
"""

import concurrent.futures

from dash import Dash, dash_table, html
import pandas as pd
import typing
from urllib.error import URLError, HTTPError
import urllib.request
import xml.etree.ElementTree as ET

#
# List of URLs to ISDE nodes
#

isde_nodes: list = ["http://geonetwork.maynoothuniversity.ie:8080/geonetwork",
                    "https://gis.epa.ie/geonetwork/srv/eng/catalog.search",
                    "http://spatial.dcenr.gov.ie/GeologicalSurvey/geonetwork ",
                    "http://pips.ucc.ie/geonetwork ",
                    "http://data.marine.ie/geonetwork/srv/eng/catalog.search",
                    "http://www.isde.ie/geonetwork/srv/eng/catalog.search",
                    "http://metadata.biodiversityireland.ie/geonetwork",
                    "http://data.ahg.gov.ie/geonetwork"]


def get_number_of_records_from_csw(csw_base_url: str) -> typing.Union[int,
                                                                      None]:
    """
    Extracts the number of MD_Metadata records available from a given instance
    of an Open Geospatial Consortium Catalog Service for the Web

    :param csw_base_url: The base URL of the OGC Catalog Service for the Web to
                         call for the GetRecords result
    :type csw_base_url: str
    :return: An integer value representing the number of records available from
             the CSW at csw_base_url, or None if an HTTPError is encountered
             or the CSW response could not be properly parsed
    :rtype: int or None
    """
    get_records_csw_get_str: str = \
        "?SERVICE=CSW" + \
        "&VERSION=2.0.2" + \
        "&REQUEST=GetRecords" + \
        "&RESULTTYPE=results" + \
        "&OUTPUTFORMAT=application/xml" + \
        "&CONSTRAINTLANGUAGE=FILTER" + \
        "&TYPENAMES=gmd:MD_Metadata"
    try:
        with urllib.request.urlopen(
                "{}{}".format(csw_base_url, get_records_csw_get_str)) as resp:
            get_records_et: ET.Element = ET.fromstring(resp.read().decode())
        return int(get_records_et[1].attrib["numberOfRecordsMatched"])
    except URLError:
        return None
    except IndexError:
        return None
    except ET.ParseError:
        return None


#
# Declares a number of variables that are used to build the DataFrame later
#

response_codes: list = []
node_record_count: list = []
node: str
this_node_record_count: int

#
# Check the health of an ISDE node
#

for node in isde_nodes:
    try:
        with urllib.request.urlopen(node) as resp:
            response_codes.append(resp.status)
        try:
            this_node_record_count = 0
            if node.find("srv") > 0:
                sitemap_url: str = ("{}portal.sitemap".format(
                    node.split("catalog.search")[0]).replace("eng", "api"))
                with urllib.request.urlopen(sitemap_url) as resp:
                    sitemap_et: ET.Element = ET.fromstring(
                        resp.read().decode())
                for child in sitemap_et:
                    this_node_record_count += 1
                node_record_count.append(this_node_record_count)
            else:
                node_record_count.append(
                    get_number_of_records_from_csw(
                        "{}geonetwork/srv/eng/csw".format(
                            node.split("geonetwork")[0])))
        except URLError:
            node_record_count.append(get_number_of_records_from_csw(
                "{}csw".format(node.split("catalog.search")[0])))
        except ET.ParseError:
            node_record_count.append(None)
    except HTTPError as error:
        response_codes.append(error.code)
        node_record_count.append(None)
    except URLError:
        response_codes.append(None)
        node_record_count.append(None)
    except TimeoutError:
        response_codes.append(None)
        node_record_count.append(None)
    except OSError:
        response_codes.append(None)
        node_record_count.append(None)

#
# A list of Boolean values, True if the HTTP response code is 200, False
# otherwise
#

node_up: list = [True if x == 200 else False for x in response_codes]

#
# A list of integer values, True if the HTTP response code is 200, False
# otherwise
#

node_up_int: list = [1 if x == 200 else 0 for x in response_codes]

#
# Build a Pandas DataFrame to describe the health of the ISDE network
#

node_health: pd.DataFrame = pd.DataFrame(list(zip(isde_nodes,
                                                  response_codes,
                                                  node_up,
                                                  node_up_int,
                                                  node_record_count)),
                                         columns=["ISDE Node",
                                                  "HTTP Response Code",
                                                  "Is Node Up",
                                                  "Is Node Up Integer",
                                                  "Number of Records"])

print(node_health)

app: Dash = Dash(__name__)
app.layout = html.Div(
    children=[html.H1("Irish Spatial Data Exchange Network Monitoring"),
              html.H2("Network Status"),
              dash_table.DataTable(node_health.to_dict("records"),
                                   [{"name": "ISDE Node",
                                     "id": "ISDE Node",
                                     "type": "text"},
                                    {"name": "HTTP Response Code",
                                     "id": "HTTP Response Code",
                                     "type": "numeric"},
                                    {"name": "Number of Records",
                                     "id": "Number of Records",
                                     "type": "numeric"}],
                                   style_data_conditional=[
                                       {
                                           'if': {
                                               'filter_query':
                                               '{Is Node Up Integer} != 1'
                                           },
                                           'backgroundColor': '#FF4136',
                                           'color': 'white'
                                       }])])

if __name__ == '__main__':
    app.run_server(debug=True)
