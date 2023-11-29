from owslib.wcs import WebCoverageService
import requests
import dotenv
import os
import xml.etree.ElementTree as ET


class GeoserverConfig():
    def __init__(self) -> None:
        
        self.env = dotenv.load_dotenv()
        self.geoserver_wcs = os.getenv("NOAA_WCS_GET")
        self.noaa_wksp_url = self.geoserver_wcs
        self.wcs = WebCoverageService(self.noaa_wksp_url)

    def get_coverages_in_workspace(self):
        coverages = [i for i in self.wcs.contents]
        meta_data = self.wcs.contents[coverages[0]]
        dir_meta = [i for i in dir(meta_data) if i[:1] != '_']


    def xmlGetCapBoundingBox(self):
        response = requests.get(self.geoserver_wcs)

        if response.status_code == 200:
            root = ET.fromstring(response.text)
            conents_element = root.find('.//wcs:Contents', namespaces={'wcs': 'http://www.opengis.net/wcs/2.0'})
            print(ET.tostring(conents_element, encoding='utf-8').decode('utf-8'))
            print(conents_element)
        else:
            print(f"Error: {response.status_code} - {response.text}")

            



