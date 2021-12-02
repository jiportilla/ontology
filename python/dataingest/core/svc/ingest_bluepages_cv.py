import requests
import textract
import time
import sys
import os
import re
import shutil
import tempfile
import plac
from datamongo import BaseMongoClient
from datamongo import CendantCollection
from base import BaseObject
from base import MandatoryParamError

class ExtractBluepagesCV():
    """ Extract CVs from Bluepages Storage
    The list of records that have a CV listed in bluepages are extracted from "ingest_bluepages_api" collection and then
    iterated over to download the CV to a created /tmp folder beofre calling a parse function which leverage textract
    to read the contents into a dictionary and write the contents to collectionname defined
    Takes 1 parameters whether to purge the collection of documents eg
    The below will delete any existing documents in the ingest_bluepages_cv collection

    python ingest_bluepages_cv.py 0 50 purge

    The below items need to be installed or configured for textract:
     - antiword - sudo apt install antiword
     - tesseract - sudo apt-get install tesseract-ocr
     - unrtf - sudo apt install unrtf
     - textract - pip install git+https://github.com/deanmalmgren/textract.git (conda only has an old version)

    """

    script_Start = time.time()
    baseurl = "https://w3-services1.w3-969.ibm.com/myw3/unified-profile/v1/resume/"
    collectionname = "ingest_bluepages_cv"

    # Generate a unique temporary folder to download and process the files to under /tmp
    with tempfile.TemporaryDirectory() as CVdirectory:
        print('created temporary directory', CVdirectory)

    def __init__(self):
        BaseObject.__init__(self, __name__)

    def exists(obj, chain):
        _key = chain.pop(0)
        if _key in obj:
            return ExtractBluepagesCV.exists(obj[_key], chain) if chain else obj[_key]

    def getcnumlist(self,mongo_client):
        """ Builds a list of CNUMS which contain a resume and shoulc be used in ingest bluepages CV routine. """
        self.logger.info("Reading data from ingest_bluepages_api collection")

        cc = CendantCollection(some_collection_name='ingest_bluepages_api',some_base_client=mongo_client)
        records = cc.all(as_list=True)

        self.logger.info("Building list of  CNUMs with resume attached in bluepages")
        resumelist = []
        for i in range(len(records)):
            if ExtractBluepagesCV.exists(records[i], ['data', 'content', 'user', 'expertise', 'resume']):
                resumelist.append(records[i]['id'])

        self.logger.info(str(len(resumelist)) + " records downloaded from ingest_bluepages_api collection ")

        return resumelist


    def parseCV(self, documents):
        """Function to parse downloaded CVs"""

        # iterate over fles in the download directory and use textract to extract the body of text and clean it of basic
        # items before inserting to the list of records
        for file in os.listdir(self.CVdirectory):
            self.logger.info("Iterating over downloaded CV " + file +" and adding to collection for DB loading")
            filename, file_extension = os.path.splitext(os.fsdecode(file))
            file = self.CVdirectory +"/"+ filename+file_extension
            try:
                text = textract.process(file, method="pdfminer", encoding='utf_8').decode("utf8")
                clean = text.replace('\n', ' ').replace('\r', ' ').replace('\'', '').replace('\t', '')
                d = next(item for item in documents if item['CNUM'] == filename)
                d['body'] = clean
            except:
                self.logger.error("Error on " + filename+file_extension + ", Removing from collection")
                documents = [i for i in documents if not (i['CNUM'] == filename)]
                continue

        return documents


    def downloadCV(self, cnums, urltemplate, downloadlist):
        """ Function to download CV from bluepages """

        count = 0
        regex = r'filename=\"((.+)\.(.+))\"' # regex that matches filename and extension
        noextregex = r'\"(.+)\"' # regex to be used if there is no extension

        # Check if output folder exists, if not then create
        if not os.path.exists(self.CVdirectory):
            os.mkdir(self.CVdirectory)

        for row in cnums:
            CVurl = urltemplate + row
            r = requests.get(CVurl,stream=True)
            self.logger.info("Downloading CV for " + row + " -- status code: " + str(r.status_code))

            # if the requests object gives a status code other than 404 then we process the download
            if r.status_code == 200:
                count += 1
                content_disposition = r.headers.get('Content-Disposition')
                extension = re.findall(regex, content_disposition) # using the regex find the extension type

                if not extension:
                    # if the regex to find filename and extension is empty it usually means that the  file was saved with
                    # no extention and therefore we force the extention to be .txt and create the extension list based on it
                    noextension = re.findall(noextregex, content_disposition)
                    extension =  [(noextension[0], noextension[0], 'txt')]

                # generate the filename ot be a combination of cnum and the previously defined ext
                filename = self.CVdirectory + "/" + row + "." + extension[0][2]

                with open(filename, 'wb') as f:
                    f.write(r.content)

                #Generate the list of files that have been downloaded based on the above and default the body to blank
                filerecord = {"CNUM": row, "file_name": extension[0][0], "file_type": extension[0][2],
                              "source": urltemplate + row, "ts": time.time(), "body": ""}
                downloadlist.append(filerecord.copy())

        self.logger.info(str(count) + " CVs downloaded")

        return downloadlist

    def deletecollectioncontent(self,mongo_client):
        """ Function to delete the collection documents from the mongo DB"""

        col = CendantCollection(some_db_name="cendant", some_collection_name=self.collectionname, some_base_client=mongo_client)
        col.delete()

    def writebluepagescvstodb(self, listofcvs, mongo_client):
        """ Function to write downloaded and parsed CVs in a list to the mongo DB"""

        # connect to the Cendent MongoDB and write records to the collections defined earlier
        total_persisted = 0
        col = CendantCollection(some_db_name="cendant", some_collection_name=self.collectionname, some_base_client=mongo_client)
        col.insert_many(listofcvs)
        total_persisted += len(listofcvs)
        self.logger.info("Number of records persisted " + str(total_persisted))

    def removedownloads(self):
        # remove all html and subfolders created in the CVdirectory folder
        shutil.rmtree(self.CVdirectory)


def main (clear_existing):
    self = ExtractBluepagesCV()

    self.logger.info('=========================================================')
    self.logger.info('        Download of CVs from Bluepages started           ')
    self.logger.info('=========================================================')

    mongo_client_cloud = BaseMongoClient(server_alias="CLOUD")

    cnumlist = ExtractBluepagesCV.getcnumlist(self,mongo_client_cloud)

    bpcvcollection = []
    bpcvcollection = ExtractBluepagesCV.downloadCV(self,cnumlist,self.baseurl,bpcvcollection)

    bpcvcollection = ExtractBluepagesCV.parseCV(self,bpcvcollection)

    if not bpcvcollection:
        self.logger.info("The list of data to be written to the DB is empty, script exiting and removing downloaded files")
        ExtractBluepagesCV.removedownloads(self)
        sys.exit(1)
    else:
        clear_existing = str(clear_existing).lower()
        if clear_existing == "purge":
            numpurged = ExtractBluepagesCV.deletecollectioncontent(self,mongo_client_cloud)
            self.logger.info(str(numpurged) + " Records purged from : " + str(self.collectionname))
        elif clear_existing == "nopurge":
            self.logger.info("No Records to be purged from : " + str(self.collectionname))
        else:
            raise MandatoryParamError("Select \"purge\" or \"nopurge\" for the existing collection")

        ExtractBluepagesCV.writebluepagescvstodb(self,bpcvcollection,mongo_client_cloud)

    ExtractBluepagesCV.removedownloads(self)

    self.script_End = time.time()
    self.logger.info("Overall Script Duration: {:.2f} minutes".format((self.script_End - self.script_Start) / 60))

if __name__ == '__main__':
    plac.call(main)
