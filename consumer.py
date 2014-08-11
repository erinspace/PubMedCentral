''' Consumer for PubMed Central '''

from lxml import etree
from xml.etree import ElementTree
from datetime import date, timedelta
import requests
from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

TODAY = date.today()
NAME = "pubmedcentral"

def consume(days_back=1):
    start_date = TODAY - timedelta(days_back)
    base_url = 'http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=ListRecords'
    # url = base_url + str(start_date)
    pmc_request = base_url + '&metadataPrefix=pmc&from={}'.format(str(TODAY))
    oai_dc_request = base_url + '&metadataPrefix=oai_dc&from{}'.format(str(TODAY))

    pmc_doc = get_xml(pmc_request)
    pmc_record = pmc_doc.xpath('//record')

    oai_doc = get_xml(oai_dc_request)

    oai_dc_namespaces = {'dc': 'http://purl.org/dc/elements/1.1/', 
                'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
                'ns0': 'http://www.openarchives.org/OAI/2.0/'}


    oai_records = doc.xpath('//ns0:record', namespaces=namespaces)

    ## add resumption token support
    

    xml_list = []
    for record in records:
        doc_id = record.xpath('ns0:header/ns0:identifier', namespaces=namespaces)[0].text
        record = ElementTree.tostring(record)
        record = '<?xml version="1.0" encoding="UTF-8"?>\n' + record
        xml_list.append(RawDocument({
                    'doc': record,
                    'source': NAME,
                    'doc_id': doc_id,
                    'filetype': 'xml'
                }))

    return xml_list

def get_records(url, namespace=None, record_namespace=''):
    data = requests.get(url)
    doc = etree.XML(data.content)
    record = doc.xpath('//{record_namespace}record', namespaces=namespace)
    return record

    ## TODO: fix if there are no records found... what would the XML look like?

def normalize(raw_doc, timestamp):
    # raw_doc = raw_doc.get('doc')
    # doc = etree.XML(raw_doc)

    # namespaces = {'dc': 'http://purl.org/dc/elements/1.1/', 
    #             'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
    #             'ns0': 'http://www.openarchives.org/OAI/2.0/'}

    # contributors = doc.findall('ns0:metadata/oai_dc:dc/dc:creator', namespaces=namespaces)
    # contributor_list = []
    # for contributor in contributors:
    #     contributor_list.append({'full_name': contributor.text, 'email':''})
    # title = doc.findall('ns0:metadata/oai_dc:dc/dc:title', namespaces=namespaces)

    # doc_id = doc.xpath('ns0:header/ns0:identifier', 
    #                             namespaces=namespaces)[0].text

    # ## TODO: make this an actual absttract maybe by going to the source...
    # try: 
    #     description = doc.xpath('ns0:metadata/oai_dc:dc/dc:description', namespaces=namespaces)[0].text
    # except IndexError:
    #     description = "No abstract available"

    # date_created = doc.xpath('ns0:metadata/oai_dc:dc/dc:date', namespaces=namespaces)[0].text

    # tags = doc.xpath('//dc:subject/node()', namespaces=namespaces)

    # normalized_dict = {
    #         'title': title[0].text,
    #         'contributors': contributor_list,
    #         'properties': {},
    #         'description': description,
    #         'meta': {},
    #         'id': doc_id,
    #         'tags': tags,
    #         'source': NAME,
    #         'date_created': date_created,
    #         'timestamp': str(timestamp)
    # }

    # return NormalizedDocument(normalized_dict)
    pass

consume()        

# if __name__ == '__main__':
#     print(lint(consume, normalize))





## Notes
# 25
# 25
# 25
# 12
# 87 with pmc data


# 50 
# 50
# 50
# 6
# 156 with dc_oai data