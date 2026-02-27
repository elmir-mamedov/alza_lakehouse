import httpx
from lxml import etree
from io import BytesIO
from curl_cffi.requests import AsyncSession # mimics browser session

async def fetch_xml(url: str) -> bytes:
    async with AsyncSession() as session:
        r = await session.get(url, impersonate="chrome120")
        r.raise_for_status()
        return r.content

def parse_sitemap(xml_bytes: bytes):
    # Use recover=True so lxml can handle broken characters like & in URLs
    parser = etree.XMLParser(recover=True)
    root = etree.parse(BytesIO(xml_bytes), parser=parser).getroot()

    ns = {"ns": root.nsmap.get(None)} if None in root.nsmap else {}

    # If sitemap index
    if root.tag.endswith("sitemapindex"):
        return [
            loc.text
            for loc in root.findall("ns:sitemap/ns:loc", namespaces=ns)
        ]

    # If regular sitemap
    if root.tag.endswith("urlset"):
        return [
            loc.text
            for loc in root.findall("ns:url/ns:loc", namespaces=ns)
        ]

    return []
