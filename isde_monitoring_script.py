import concurrent.futures

from dash import Dash, dash_table, html
import pandas as pd
from urllib.error import URLError, HTTPError
import urllib.request
import xml.etree.ElementTree as ET

isde_nodes: list = ["http://geonetwork.maynoothuniversity.ie:8080/geonetwork",
                    "https://gis.epa.ie/geonetwork/srv/eng/catalog.search",
                    "http://spatial.dcenr.gov.ie/GeologicalSurvey/geonetwork ",
                    "http://pips.ucc.ie/geonetwork ",
                    "http://data.marine.ie/geonetwork/srv/eng/catalog.search",
                    "http://www.isde.ie/geonetwork/srv/eng/catalog.search",
                    "http://metadata.biodiversityireland.ie/geonetwork",
                    "http://data.ahg.gov.ie/geonetwork"]

response_codes: list = []
node_record_count: list = []
node: str
this_node_record_count: int

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
                    sitemap_et: ET.Element = ET.fromstring(resp.read().decode())
                for child in sitemap_et:
                    this_node_record_count += 1
                node_record_count.append(this_node_record_count)
            else:
                node_record_count.append(None)
        except URLError as error:
            node_record_count.append(None)
        except ET.ParseError as error:
            node_record_count.append(None)
    except HTTPError as error:
        response_codes.append(error.code)
        node_record_count.append(None)
    except URLError as error:
        response_codes.append(None)
        node_record_count.append(None)
    except TimeoutError as error:
        response_codes.append(None)
        node_record_count.append(None)
    except OSError as error:
        response_codes.append(None)
        node_record_count.append(None)

node_up: list = [True if x == 200 else False for x in response_codes]

node_up_int: list = [1 if x == 200 else 0 for x in response_codes]

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
                                   [{"name": "ISDE Node", "id": "ISDE Node", "type": "text"},
                                    {"name": "HTTP Response Code", "id": "HTTP Response Code", "type": "numeric"},
                                    {"name": "Number of Records", "id": "Number of Records", "type": "numeric"}],
                                   style_data_conditional=[
                                       {
                                           'if': {
                                               'filter_query': '{Is Node Up Integer} != 1'
                                           },
                                           'backgroundColor': '#FF4136',
                                           'color': 'white'
                                       }])])

if __name__ == '__main__':
    app.run_server(debug=True)
