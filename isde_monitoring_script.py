"""
Displays a monitoring dashboard to display the status of the nodes in the Irish
Spatial Data Exchange network.

Dependencies are pandas, dash and xmlschema

:author: Adam Leadbetter (@adamml)
"""

import concurrent.futures

from dash import Dash, dash_table, html
from datetime import datetime
from itertools import repeat
from lxml import etree as lET
import pandas as pd
import typing
from urllib.error import URLError, HTTPError
import urllib.request
import xml.etree.ElementTree as ET
import xmlschema

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

isde_sitemap_url: str = "https://www.isde.ie/geonetwork/srv/api/portal.sitemap"

xml_schema = xmlschema.XMLSchema(
    'http://schemas.opengis.net/iso/19139/20060504/gmd/gmd.xsd')


def fetch_schema():
    pass


def validate_isde_record(record_url: str,
                         schema: xmlschema.XMLSchema) -> list:
    """

    :param record_url:
    :type record_url: str
    :return:
    :rtype: list
    """
    try:
        with urllib.request.urlopen(record_url) as resp:
            record_et: ET.Element = ET.fromstring(resp.read().decode())
        schema.validate(record_et)
    except ET.ParseError:
        return [record_url, None, None]
    except HTTPError as ee:
        print("Could not open for validating: {} Reason: {} ...".format(record_url, str(ee)))
        return [None, None, None]
    except URLError as ee:
        print("Could not open for validating: {} Reason: {} ...".format(record_url, str(ee)))
        return [None, None, None]
    except xmlschema.validators.exceptions.XMLSchemaValidatorError as ee:
        return [None, record_url, str(ee)]
    except xmlschema.exceptions.XMLSchemaKeyError as ee:
        return [None, record_url, str(ee)]
    return [None, None, None]


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


def get_most_recent_created_modified_from_csw(csw_base_url: str,
                                              number_of_records: int) -> list:
    """
    Extracts the dates of the most recently created and most recently modified
    MD_Metadata records from an Open Geospatial Consortium Catalog Service for
    the Web (ODC-CSW) endpoint

    :param csw_base_url: The base URL of the OGC Catalog Service for the Web to
                         call for the GetRecords result
    :type csw_base_url: str
    :param number_of_records: The number of records to be sampled, should be set
                              to at least the number of records expected to be
                              returned from the OGC-CSW endpoint
    :type number_of_records: int
    :return: A two-element list, the first element being the date of the most
             recently created MD_Metadata record on the OGC-CSW endpoint, the
             second element being the date of the most recently modified
             record on the same endpoint. The date is returned as a %Y-%m-%d
             string. Either element may be None if an error is encountered or
             if the element set for the endpoint does not support the date
             type requested.
    :rtype: list
    """
    result = [None, None]
    get_all_records_csw_get_str: str = \
        "?SERVICE=CSW" + \
        "&VERSION=2.0.2" + \
        "&REQUEST=GetRecords" + \
        "&RESULTTYPE=results" + \
        "&OUTPUTFORMAT=application/xml" + \
        "&CONSTRAINTLANGUAGE=FILTER" + \
        "&TYPENAMES=gmd:MD_Metadata" + \
        "&ELEMENTSETNAME=full" + \
        "&MAXRECORDS={}".format(str(number_of_records + 1))
    try:
        child: ET.Element
        with urllib.request.urlopen("{}{}".format(csw_base_url, get_all_records_csw_get_str)) as resp:
            get_records: ET.Element = ET.fromstring(resp.read().decode())
        for child in get_records[1]:
            for prop in child:
                if prop.tag == "{http://purl.org/dc/elements/1.1/}date":
                    if not result[1]:
                        result[1] = prop.text.split('T')[0]
                    else:
                        if datetime.strptime(
                                result[1], "%Y-%m-%d") < datetime.strptime(
                                prop.text.split("T")[0], "%Y-%m-%d"):
                            result[1] = prop.text.split('T')[0]
                elif prop.tag == "{http://purl.org/dc/terms/}modified":
                    if not result[1]:
                        result[1] = prop.text.split('T')[0]
                    else:
                        if datetime.strptime(
                                result[1], "%Y-%m-%d") < datetime.strptime(
                                prop.text.split("T")[0], "%Y-%m-%d"):
                            result[1] = prop.text.split('T')[0]
                elif prop.tag == "{http://purl.org/dc/terms/}created":
                    if not result[0]:
                        result[0] = prop.text.split('T')[0]
                    else:
                        if datetime.strptime(
                                result[0], "%Y-%m-%d") < datetime.strptime(
                                prop.text.split("T")[0], "%Y-%m-%d"):
                            result[0] = prop.text.split('T')[0]
        return result
    except URLError:
        return [None, None]
    except ET.ParseError:
        return [None, None]
    except IndexError:
        return [None, None]


def get_node_health(node: str) -> list:
    """
    Tests the health of a node in the Irish Spatial Data Exchange network, if
    that node exposes an Open Geospatial Consortium Catalog Service for the
    Web (OGC CSW) endpoint.

    :param node: The URL of the OGC CSW endpoint which is to be tested
    :type node: str
    :return: A four-element list, the first element is the HTTP header response
             code of the server being called; the second element is the number
             of MD_Metadata records contained on that server; the third element
             is the most recent metadata created data for the endpoint; the
             fourth element is the most recent modified date for the
             MD_Metadata records at the endpoint. All elements are normally of
             type int, but may be None if an error has occurred
    :rtype: list
    """
    this_node_record_count: int
    result: list = []
    try:
        with urllib.request.urlopen(node) as resp:
            result.append(resp.status)
        try:
            this_node_record_count = 0
            if node.find("srv") > 0:
                sitemap_url: str = ("{}portal.sitemap".format(
                    node.split("catalog.search")[0]).replace("eng", "api"))
                with urllib.request.urlopen(sitemap_url) as resp:
                    sitemap_et: ET.Element = ET.fromstring(
                        resp.read().decode())
                lastmod = None
                for child in sitemap_et:
                    this_node_record_count += 1
                    for prop in child:
                        if prop.tag == "{http://www.sitemaps.org/schemas/" + \
                                "sitemap/0.9}lastmod":
                            if not lastmod:
                                lastmod = prop.text.split("T")[0]
                            elif datetime.strptime(lastmod, "%Y-%m-%d") < \
                                    datetime.strptime(prop.text.split("T")[0],
                                                      "%Y-%m-%d"):
                                lastmod = prop.text.split("T")[0]
                result.append(this_node_record_count)
                result.append(None)
                result.append(lastmod)
            else:
                result.append(
                    get_number_of_records_from_csw(
                        "{}geonetwork/srv/eng/csw".format(
                            node.split("geonetwork")[0])))
                result.extend(get_most_recent_created_modified_from_csw(
                    "{}geonetwork/srv/eng/csw".format(
                        node.split("geonetwork")[0]), result[1]))
        except URLError:
            result.append(get_number_of_records_from_csw(
                "{}csw".format(node.split("catalog.search")[0])))
            result.extend(get_most_recent_created_modified_from_csw(
                "{}csw".format(node.split("catalog.search")[0]), result[1]))
        except ET.ParseError:
            result.append(None)
            result.append(None)
            result.append(None)
    except HTTPError as error:
        result.append(error.code)
        result.append(None)
        result.append(None)
        result.append(None)
    except URLError:
        result.append(None)
        result.append(None)
        result.append(None)
        result.append(None)
    except TimeoutError:
        result.append(None)
        result.append(None)
        result.append(None)
        result.append(None)
    except OSError:
        result.append(None)
        result.append(None)
        result.append(None)
        result.append(None)
    return result


#
# Test the health of the nodes
#
response_codes: list = []
node_record_count: list = []
last_modified: list = []
with concurrent.futures.ThreadPoolExecutor(max_workers=16) as exc:
    res = exc.map(get_node_health, isde_nodes)
for r in res:
    response_codes.append(r[0])
    node_record_count.append(r[1])
    last_modified.append(r[3])

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
                                                  node_record_count,
                                                  last_modified)),
                                         columns=["ISDE Node",
                                                  "HTTP Response Code",
                                                  "Is Node Up",
                                                  "Is Node Up Integer",
                                                  "Number of Records",
                                                  "Last Modified"])


#
# Check the XML structure for each record at the ISDE central node
#

bad_xml: list = []
invalid_xml: list = []
invalid_reason: list = []

with urllib.request.urlopen(isde_sitemap_url) as resp:
    isde_sitemap: ET.Element = ET.fromstring(resp.read().decode())
isde_urls = ["{}/formatters/xml".format(
    url[0].text.replace(".ie:/", ".ie/")).replace("http://", "https://www.") for url in isde_sitemap]

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as exc:
    res = exc.map(validate_isde_record, isde_urls, repeat(xml_schema))
for r in res:
    if r[0]:
        bad_xml.append(r[0])
    if r[1]:
        invalid_xml.append(r[1])
        invalid_reason.append(r[2])

bad_xml_df: pd.DataFrame = pd.DataFrame(bad_xml, columns=["URL"])

invalid_xml_df: pd.DataFrame = pd.DataFrame(list(zip(invalid_xml,
                                                     invalid_reason)),
                                            columns=["URL",
                                                     "Reason"])

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
                                    {"name": "Last Modified",
                                     "id": "Last Modified",
                                     "type": "text"},
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
                                       }]),
              html.H2("Malformed XML records"),
              dash_table.DataTable(bad_xml_df.to_dict("records"),
                                   [{"name": "Record URL",
                                     "id": "URL",
                                     "type": "text"}]),
              html.H2("Invalid Metadata Records"),
              dash_table.DataTable(invalid_xml_df.to_dict("records"),
                                   [{"name": "Record URL",
                                     "id": "URL",
                                     "type": "text"},
                                    {"name": "Reason Invalid",
                                     "id": "Reason",
                                     "type": "text"}])])

if __name__ == '__main__':
    app.run_server(debug=True)
