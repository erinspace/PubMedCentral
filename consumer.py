''' Consumer for PubMed Central '''

from lxml import etree
from datetime import date, timedelta
import requests
from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

TODAY = date.today()
NAME = "pubmedcentral"

def consume(days_back=0):
    start_date = TODAY - timedelta(days_back)
    base_url = 'http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=ListRecords'
    pmc_request = base_url + '&metadataPrefix=pmc&from={}'.format(str(start_date))
    oai_dc_request = base_url + '&metadataPrefix=oai_dc&from={}'.format(str(start_date))

    namespaces = {'dc': 'http://purl.org/dc/elements/1.1/', 
                'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
                'ns0': 'http://www.openarchives.org/OAI/2.0/'}

    oai_records = get_records(oai_dc_request, namespaces)
    pmc_records = get_records(pmc_request, namespaces)

    records =  pmc_records + oai_records

    xml_list = []
    for record in records:
        doc_id = record.xpath('ns0:header/ns0:identifier', namespaces=namespaces)[0].text
        record = etree.tostring(record)
        record = '<?xml version="1.0" encoding="UTF-8"?>\n' + record
        xml_list.append(RawDocument({
                    'doc': record,
                    'source': NAME,
                    'doc_id': doc_id,
                    'filetype': 'xml'
                }))
    return xml_list

def get_records(url, namespace):
    data = requests.get(url)
    doc = etree.XML(data.content)
    records = doc.xpath('//ns0:record', namespaces=namespace)
    token = doc.xpath('//ns0:resumptionToken/node()', namespaces=namespace)

    if len(token) == 1: 
        base_url = 'http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=ListRecords&resumptionToken=' 
        url = base_url + token[0]
        records += get_records(url, namespace={'ns0': 'http://www.openarchives.org/OAI/2.0/'})

    return records


def normalize(raw_doc, timestamp):
    raw_doc = raw_doc.get('doc')
    doc = etree.XML(raw_doc)

    namespaces = {'dc': 'http://purl.org/dc/elements/1.1/', 
                'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
                'ns0': 'http://www.openarchives.org/OAI/2.0/',
                'arch': 'http://dtd.nlm.nih.gov/2.0/xsd/archivearticle'}

    title = doc.xpath('//dc:title/node()', namespaces=namespaces)
    if len(title) == 0:
        title = doc.xpath('//arch:title-group/arch:article-title/node()', namespaces=namespaces)
        print title

    # import pdb; pdb.set_trace()

    # print title
    # title = doc.findall('ns0:metadata/oai_dc:dc/dc:title', namespaces=namespaces)

    # contributors = doc.findall('ns0:metadata/oai_dc:dc/dc:creator', namespaces=namespaces)
    # contributor_list = []
    # for contributor in contributors:
    #     contributor_list.append({'full_name': contributor.text, 'email':''})

    # doc_id = doc.xpath('ns0:header/ns0:identifier', 
    #                             namespaces=namespaces)[0].text

    # ## TODO: make this an actual absttract maybe by going to the source...
    # try: 
    #     description = doc.xpath('ns0:metadata/oai_dc:dc/dc:description', namespaces=namespaces)[0].text
    # except IndexError:
    #     description = "No abstract available"

    # date_created = doc.xpath('ns0:metadata/oai_dc:dc/dc:date', namespaces=namespaces)[0].text

    # tags = doc.xpath('//dc:subject/node()', namespaces=namespaces)
    try:
        normalized_dict = {
                
                'title': title[0],

                'contributors': [{'full_name': 'person', 'email':'email'}],
                'properties': {},
                'description': 'stuff',
                'meta': {},
                'id': 'doc_id',
                'tags': ['some', 'tags'],
                'source': NAME,
                'date_created': 'date_created',
                'timestamp': str(timestamp)
        }
    except IndexError:
        normalized_dict = {
                'title': 'error',
                'contributors': [{'full_name': 'person', 'email':'email'}],
                'properties': {},
                'description': 'stuff',
                'meta': {},
                'id': 'doc_id',
                'tags': ['some', 'tags'],
                'source': NAME,
                'date_created': 'date_created',
                'timestamp': str(timestamp)
        }
        
        print 'error in {}!!'.format(NAME)

    # print normalized_dict
    return NormalizedDocument(normalized_dict)

if __name__ == '__main__':
    print(lint(consume, normalize))
