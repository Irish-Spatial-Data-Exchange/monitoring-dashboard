"""
Creates a report detailing the health of the Irish Spatial Data Exchange
network as a Markdown document.

Dependency is xmlschema

:author: Adam Leadbetter (@adamml)
"""
import concurrent.futures
import datetime
import itertools
import typing
import urllib.request
import xml.etree.ElementTree as ET
import xmlschema

xml_schema = xmlschema.XMLSchema(
    'http://schemas.opengis.net/iso/19139/20060504/gmd/gmd.xsd')
"""Location of the XML Schema to validate the individual ISDE records
against"""

isde_nodes: list = ["http://geonetwork.maynoothuniversity.ie:8080/geonetwork",
                    "https://gis.epa.ie/geonetwork/srv/eng/catalog.search",
                    "http://spatial.dcenr.gov.ie/GeologicalSurvey/geonetwork ",
                    "http://pips.ucc.ie/geonetwork ",
                    "http://data.marine.ie/geonetwork/srv/eng/catalog.search",
                    "http://www.isde.ie/geonetwork/srv/eng/catalog.search",
                    "http://metadata.biodiversityireland.ie/geonetwork",
                    "http://data.ahg.gov.ie/geonetwork"]
"""A list of the URLs of the nodes which make up the Irish Spatial Data
Exchange network"""

dataset_title: str = (
            ".//{http://www.isotc211.org/2005/gmd}identificationInfo/" +
            "{http://www.isotc211.org/2005/gmd}MD_DataIdentification/" +
            "{http://www.isotc211.org/2005/gmd}citation/" +
            "{http://www.isotc211.org/2005/gmd}CI_Citation/" +
            "{http://www.isotc211.org/2005/gmd}title/" +
            "{http://www.isotc211.org/2005/gco}CharacterString")
"""XSPath query to get a dataset title from a ISO 19115 / 19139 record"""

def parse_isde_sitemap():
    """Gets the full list of records from the ISDE sitemap

    :return: A list of the URLs to records on the main ISDE geonetwork
    :rtype: list of str
    """
    with urllib.request.urlopen(
            "https://www.isde.ie/geonetwork/srv/" +
            "api/portal.sitemap") as rss_response:
        rss_tree = ET.fromstring(rss_response.read().decode("utf-8"))
        return ["{}/formatters/xml".format(
            url[0].text.replace(".ie:/", ".ie/").replace(
                "http://", "https://www.")) for url in rss_tree]


def validate_isde_record(record_url, schema):
    try:
        with urllib.request.urlopen(record_url) as record_response:
            record = ET.fromstring(record_response.read())
        title: str = ""
        e: typing.List[ET.Element] = record.findall(dataset_title)
        for t in e:
            title = str(t.text)
        if title == "":
            title = None
        schema.validate(record)
        return[1, None, None, None, title]
    except ET.ParseError:
        return [1, record_url, None, None, None]
    except urllib.error.URLError:
        return [0, None, record_url, None, None]
    except xmlschema.validators.exceptions.XMLSchemaValidatorError as ee:
        return [1, None, record_url, str(ee), title]
    except xmlschema.exceptions.XMLSchemaKeyError as ee:
        return [1, None, record_url, str(ee), title]


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
                            elif datetime.datetime.strptime(lastmod,
                                                            "%Y-%m-%d") < \
                                    datetime.datetime.strptime(prop.text.split(
                                        "T")[0], "%Y-%m-%d"):
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
        except urllib.error.URLError:
            result.append(get_number_of_records_from_csw(
                "{}csw".format(node.split("catalog.search")[0])))
            result.extend(get_most_recent_created_modified_from_csw(
                "{}csw".format(node.split("catalog.search")[0]), result[1]))
        except ET.ParseError:
            result.append(None)
            result.append(None)
            result.append(None)
    except urllib.error.HTTPError as error:
        result.append(error.code)
        result.append(None)
        result.append(None)
        result.append(None)
    except urllib.error.URLError:
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
    except urllib.error.URLError:
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
    :param number_of_records: The number of records to be sampled, should be
                              set to at least the number of records expected
                              to be returned from the OGC-CSW endpoint
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
    
    try:
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
    except TypeError:
        get_all_records_csw_get_str: str = \
            "?SERVICE=CSW" + \
            "&VERSION=2.0.2" + \
            "&REQUEST=GetRecords" + \
            "&RESULTTYPE=results" + \
            "&OUTPUTFORMAT=application/xml" + \
            "&CONSTRAINTLANGUAGE=FILTER" + \
            "&TYPENAMES=gmd:MD_Metadata" + \
            "&ELEMENTSETNAME=full" + \
            "&MAXRECORDS=1"
    try:
        child: ET.Element
        with urllib.request.urlopen("{}{}".format(csw_base_url,
                                                  get_all_records_csw_get_str)
                                    ) as resp:
            get_records: ET.Element = ET.fromstring(resp.read().decode())
        for child in get_records[1]:
            for prop in child:
                if prop.tag == "{http://purl.org/dc/elements/1.1/}date":
                    if not result[1]:
                        result[1] = prop.text.split('T')[0]
                    else:
                        if datetime.datetime.strptime(
                                result[1], "%Y-%m-%d") < \
                                    datetime.datetime.strptime(
                                prop.text.split("T")[0], "%Y-%m-%d"):
                            result[1] = prop.text.split('T')[0]
                elif prop.tag == "{http://purl.org/dc/terms/}modified":
                    if not result[1]:
                        result[1] = prop.text.split('T')[0]
                    else:
                        if datetime.datetime.strptime(
                                result[1], "%Y-%m-%d") < \
                                    datetime.datetime.strptime(
                                prop.text.split("T")[0], "%Y-%m-%d"):
                            result[1] = prop.text.split('T')[0]
                elif prop.tag == "{http://purl.org/dc/terms/}created":
                    if not result[0]:
                        result[0] = prop.text.split('T')[0]
                    else:
                        if datetime.datetime.strptime(
                                result[0], "%Y-%m-%d") < \
                                    datetime.datetime.strptime(
                                prop.text.split("T")[0], "%Y-%m-%d"):
                            result[0] = prop.text.split('T')[0]
        return result
    except urllib.error.URLError:
        return [None, None]
    except ET.ParseError:
        return [None, None]
    except IndexError:
        return [None, None]


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

node_health = list(zip(isde_nodes, response_codes,
                       node_up, node_up_int, node_record_count,
                       last_modified))

node_health_table = ''.join(["\n| 🟢 | [{}]({}) | {} | {} | {} |".format(x[0],
                                                                        x[0],
                                                                        x[1],
                                                                        x[4],
                                                                        x[5])
                            if x[2] else "\n| 🔴 | [{}]({}) | {} | | |".format(
                                x[0], x[0], x[1])
                            for x in node_health])

with concurrent.futures.ThreadPoolExecutor(max_workers=16) as exc:
    res = exc.map(validate_isde_record, parse_isde_sitemap(
        ), itertools.repeat(xml_schema))

records_checked = 0
records_not_checked = 0
malformed_records = 0
invalid_xml = 0

records_not_checked_md_list = ""
malformed_records_md_list = ""
invalid_records_md_table = ""

for r in res:
    if r[0] == 1:
        records_checked += 1
        if r[1]:
            malformed_records_md_list += "\n- [{}]({})".format(r[1], r[1])
            malformed_records += 1
        if r[2]:
            invalid_xml += 1
            if not r[4]:
                invalid_records_md_table += "\n| [{}]({}) | {} |".format(r[2],
                                                                         r[2],
                                                                         r[3].replace(
                                                                         "\n", ""))
            else:
                invalid_records_md_table += "\n| [{}]({}) | {} |".format(r[4],
                                                                         r[2],
                                                                         r[3].replace(
                                                                         "\n", ""))
    elif r[0] == 0:
        records_not_checked += 1
        records_not_checked_md_list += "\n- [{}]({})".format(r[2], r[2])

if not records_not_checked_md_list:
    records_not_checked_md_list = "\n- None"

if not malformed_records_md_list:
    malformed_records_md_list = "\n- None"

out_str = """# ISDE Network Monitoring Report - {}

[![rebuild report](https://github.com/Irish-Spatial-Data-Exchange/monitoring-dashboard/actions/workflows/rebuild_report.yml/badge.svg)](https://github.com/Irish-Spatial-Data-Exchange/monitoring-dashboard/actions/workflows/rebuild_report.yml)

| Number of Nodes | Number of Nodes Up | Number of Nodes Down | Records Checked | Records Not Checked | Records with Malformed XML | Records with Invalid XML |
|---|---|---|---|---|---|---|
| {} | 🟢 {} | 🔴 {} | 🟢 {} | 🔴 {} | 🟠 {} | 🟠 {} |

### Contents

- [Network Status](#network-status)
- [XML Records Not Checked](#xml-records-not-checked)
- [Malformed XML Records](#malformed-xml-records)
- [Invalid XML Records](#invalid-xml-records)

## Network Status

| | Node | HTTP Response Code | Number of Records | Last Modified |
|---|---|---|---|---|{}

## XML records not checked
{}

## Malformed XML records
{}

## Invalid XML records

| Record | Invalid Reason |
|---|---|{}

""".format(
    datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
    len(isde_nodes),
    sum(node_up_int),
    (len(isde_nodes) - sum(node_up_int)),
    records_checked,
    records_not_checked,
    malformed_records,
    invalid_xml,
    node_health_table,
    records_not_checked_md_list,
    malformed_records_md_list,
    invalid_records_md_table
)

with open('report.md', 'w', encoding='utf-8') as report_file:
    report_file.write(out_str)
