''' Consumer for PubMed Central
    Takes in both metadata in dc and pmc formats '''

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

    print pmc_request
    print oai_dc_request

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

    ## title ##
    title = doc.xpath('//dc:title/node()', namespaces=namespaces)
    if len(title) == 0:
        title = doc.xpath('//arch:title-group/arch:article-title/node()', namespaces=namespaces)

    ## contributors ##
    contributors = doc.xpath('//dc:creator/node()', namespaces=namespaces)

    if len(contributors) == 0:
        surname = doc.xpath('//arch:contrib/arch:name/arch:surname/node()', namespaces=namespaces)
        given_names = doc.xpath('//arch:contrib/arch:name/arch:given-names/node()', namespaces=namespaces)
        full_names = zip(surname, given_names)
        contributors += [', '.join(names) for names in full_names]
        
        email_list = []
        email = doc.xpath('//arch:contrib/arch:email/node()', namespaces=namespaces)

        if len(email) == len(contributors):
            email_list = email
        else:
            email_list.append('')
        
        contributors = zip(contributors, email_list)

    contributor_list = []
    for contributor in contributors:
        if type(contributor) == tuple:
            contributor_list.append({'full_name': contributor[0], 'email':contributor[1]})
        else:
            contributor_list.append({'full_name': contributor, 'email':''})

    ## description ##
    description = doc.xpath('//dc:description/node()', namespaces=namespaces)
    if len(description) == 0:
        description = doc.xpath('//arch:abstract/arch:p/node()', namespaces=namespaces)

    try:
        description = description[0]
    except IndexError:
        description = 'No description available.'

    ## id ##
    id_url = ''
    id_doi = ''
    pmid = ''
    identifiers = doc.xpath('//dc:identifier/node()', namespaces=namespaces)
    if len(identifiers) > 1:
        id_url = identifiers[1]
        pmid = id_url[-8:]
    if len(identifiers) == 3:
        id_doi = identifiers[2]
    else:
        identifiers = doc.xpath('//arch:article-id/node()', namespaces=namespaces)
        id_doi = doc.xpath("//arch:article-id[@pub-id-type='doi']/node()", namespaces=namespaces)
        pmid = doc.xpath("//arch:article-id[@pub-id-type='pmid']/node()", namespaces=namespaces)
        
        if len(pmid) == 1:
            pmid = pmid[0]
            id_url = 'http://www.ncbi.nlm.nih.gov/pubmed/' + pmid

        if len(id_doi) == 1:
            id_doi = id_doi[0]
            id_url = 'http://dx.doi.org/' + id_doi

    doc_ids = {'url': id_url, 'doi': id_doi, 'service_id': pmid}
    print doc_ids

    ## tags ##

    normalized_dict = { 
        'title': title[0],

        'contributors': contributor_list,
        'properties': {},
        'description': description,
        'meta': {},
        'id': doc_ids,
        'tags': ['some', 'tags'],
        'source': NAME,
        'date_created': 'date_created',
        'timestamp': str(timestamp)
    }


    # print normalized_dict
    return NormalizedDocument(normalized_dict)

if __name__ == '__main__':
    print(lint(consume, normalize))
