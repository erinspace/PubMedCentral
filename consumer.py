''' Consumer for PubMed Central
    Takes in both metadata in dc and pmc formats '''

from lxml import etree
from datetime import date, timedelta
import requests
import time
from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument

TODAY = date.today()
NAME = "pubmedcentral"


NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/', 
            'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
            'ns0': 'http://www.openarchives.org/OAI/2.0/',
            'arch': 'http://dtd.nlm.nih.gov/2.0/xsd/archivearticle'}


def consume(days_back=0):
    start_date = TODAY - timedelta(days_back)
    base_url = 'http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=ListRecords'
    pmc_request = base_url + '&metadataPrefix=pmc&from={}'.format(str(start_date))
    oai_dc_request = base_url + '&metadataPrefix=oai_dc&from={}'.format(str(start_date))

    print 'pmc request: {0}\oai_dc request: {1}'.format(pmc_request, oai_dc_request)

    oai_records = get_records(oai_dc_request)
    pmc_records = get_records(pmc_request)
    records =  pmc_records + oai_records
    print '{} records collected...'.format(len(records))

    xml_list = []
    for record in records:
        ## TODO: make lack of contributors continue the loop
        contributors = record.xpath('//dc:creator/node()', namespaces=NAMESPACES) or record.xpath('//arch:contrib/arch:name/arch:surname/node()', namespaces=NAMESPACES)
        if not contributors:
            continue
        doc_id = record.xpath('ns0:header/ns0:identifier/node()', namespaces=NAMESPACES)[0]
        record = etree.tostring(record)
        record = '<?xml version="1.0" encoding="UTF-8"?>\n' + record
        xml_list.append(RawDocument({
                    'doc': record,
                    'source': NAME,
                    'doc_id': doc_id,
                    'filetype': 'xml'
                }))

    return xml_list


def get_records(url):
    data = requests.get(url)
    doc = etree.XML(data.content)
    records = doc.xpath('//ns0:record', namespaces=NAMESPACES)
    token = doc.xpath('//ns0:resumptionToken/node()', namespaces=NAMESPACES)

    if len(token) == 1:
        time.sleep(0.5)
        base_url = 'http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=ListRecords&resumptionToken=' 
        url = base_url + token[0]
        records += get_records(url)

    return records

def get_title(doc):
    title = doc.xpath('//dc:title', namespaces=NAMESPACES)
    if len(title) == 0:
        title = doc.xpath('//arch:title-group/arch:article-title/node()', namespaces=NAMESPACES)
        if len(title) > 1:
            full_title = ''
            for part in title:
                if type(part) == etree._Element:
                    full_title += str(part.text) + ' '
                else:
                    full_title += part + ' '
            title = full_title

    if isinstance(title, list):
        title = title[0]
        if isinstance(title, etree._Element):
            title = title.text

    title = title.strip()
    return title


def normalize(raw_doc, timestamp):
    raw_doc = raw_doc.get('doc')
    doc = etree.XML(raw_doc)

    ## title ##
    title = get_title(doc)

    ## contributors ##
    contributors = doc.xpath('//dc:creator/node()', namespaces=NAMESPACES)

    if len(contributors) == 0:
        surname = doc.xpath('//arch:contrib/arch:name/arch:surname/node()', namespaces=NAMESPACES)
        given_names = doc.xpath('//arch:contrib/arch:name/arch:given-names/node()', namespaces=NAMESPACES)
        full_names = zip(surname, given_names)
        contributors += [', '.join(names) for names in full_names]
        
        email_list = []
        email = doc.xpath('//arch:contrib/arch:email/node()', namespaces=NAMESPACES)

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

    contributor_list = contributor_list or [{'full_name': 'no contributors', 'email': ''}]

    ## description ##
    description = doc.xpath('//dc:description/node()', namespaces=NAMESPACES)
    if len(description) == 0:
        description = doc.xpath('//arch:abstract/arch:p/node()', namespaces=NAMESPACES)
    try:
        description = description[0]
    except IndexError:
        description = 'No description available.'

    ## id ##  note - pmid is collected but not used right now
    id_url = ''
    id_doi = ''
    pmid = ''
    service_id = doc.xpath('ns0:header/ns0:identifier/node()', namespaces=NAMESPACES)[0]
    identifiers = doc.xpath('//dc:identifier/node()', namespaces=NAMESPACES)
    if len(identifiers) > 1:
        id_url = identifiers[1]
        if id_url[:17] == 'http://dx.doi.org':
            id_doi = id_url[18:]
        else:
            pmid = id_url[-8:]

    if len(identifiers) == 3:
        id_doi = identifiers[2][18:]

    elif len(identifiers) == 0:
        identifiers = doc.xpath('//arch:article-id/node()', namespaces=NAMESPACES)
        id_doi = doc.xpath("//arch:article-id[@pub-id-type='doi']/node()", namespaces=NAMESPACES)
        pmid = doc.xpath("//arch:article-id[@pub-id-type='pmid']/node()", namespaces=NAMESPACES)
        
        if len(pmid) == 1:
            pmid = pmid[0]
            id_url = 'http://www.ncbi.nlm.nih.gov/pubmed/' + pmid

        if len(id_doi) == 1:
            id_doi = id_doi[0]
            id_url = 'http://dx.doi.org/' + id_doi
        if id_url == '':
            raise Exception("No url provided!")

    doc_ids = {'url': id_url, 'doi': id_doi, 'service_id': service_id}

    ## tags ##
    keywords = doc.xpath('//arch:kwd/node()', namespaces=NAMESPACES)
    tags = []
    for keyword in keywords:
        if type(keyword) == etree._Element:
            tags.append(keyword.text)
        elif keyword.strip() != '' and keyword != ')':
            tags.append(keyword)
    tags = [tag.strip().replace(' (', '') for tag in tags]

    ## date created ##
    try:
        date_created = doc.xpath('//dc:date/node()', namespaces=NAMESPACES)[0]
    except IndexError:
        date_list = doc.xpath('//arch:date[@date-type="received"]', namespaces=NAMESPACES)
        if len(date_list) == 0:
            date_list = doc.xpath('//arch:pub-date[@pub-type="epub"]', namespaces=NAMESPACES)
        year = date_list[0].find('arch:year', namespaces=NAMESPACES).text
        month = date_list[0].find('arch:month', namespaces=NAMESPACES).text.zfill(2)
        day = date_list[0].find('arch:day', namespaces=NAMESPACES).text.zfill(2)
        
        date_created = '{year}-{month}-{day}'.format(year=year, month=month, day=day)

    normalized_dict = { 
        'title': title,
        'contributors': contributor_list,
        'properties': {},
        'description': description,
        'meta': {},
        'id': doc_ids,
        'tags': tags,
        'source': NAME,
        'date_created': date_created,
        'timestamp': str(timestamp)
    }

    # print normalized_dict
    return NormalizedDocument(normalized_dict)

if __name__ == '__main__':
    print(lint(consume, normalize))
