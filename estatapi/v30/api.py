import io
from getpass import getpass
import urllib3

class Connector:

    def __init__(self, appId=None, lang="J", format="xml", gzip=True):

        if appId is None:
            appId = getpass(prompt="appId: ")
        self.appId = appId
        self.lang = lang.upper()
        self.format = format.lower()

        headers = urllib3.make_headers(accept_encoding=True) if gzip else {}
        self.http = urllib3.PoolManager(headers=headers)

        self._baseurl = "https://api.e-stat.go.jp/rest/3.0/app/"
        self._version = "e-Stat API version 3.0"

    def __repr__(self):
        appId = ("****" if len(self.appId) < 4
                 else self.appId[:4] + "*" * (len(self.appId) - 4))
        return (self._version + "\n" + "appId: " + appId
                + "\nformat: " + self.format + "\nlang: " + self.lang)

    @property
    def appId(self):
        return self._appId

    @appId.setter
    def appId(self, value):
        self._appId = value

    @property
    def lang(self):
        return self._lang

    @lang.setter
    def lang(self, value):
        if value not in ["J", "E"]:
            raise(ValueError("lang must be either [J]apanese or [E]nglish."))
        self._lang = value

    @property
    def format(self):
        return self._format

    @format.setter
    def format(self, value):
        value = value.lower()
        if value not in ["xml", "json", "jsonp", "csv"]:
            raise(ValueError("Supported format: xml, json, jsonp, or csv."))
        self._format = value

    @property
    def version(self):
        return self._version

    @property
    def baseurl(self):
        return self._baseurl

    def __request(self, http_verb, api_method, params={}):

        if self.format == "xml":
            method_string = api_method
        elif self.format in ["json", "jsonp"]:
            method_string = self.format + "/" + api_method
        else:
            method_string = api_method[:3] + "Simple" + api_method[3:]

        url = self.baseurl + method_string

        params.update({"appId": self.appId})
        r = self.http.request(http_verb, url, fields=params)

        return r.data.decode()


    def getStatsList(self, surveyYears=None, openYears=None, statsField=None,
                     statsCode=None, searchWord=None, searchKind=1,
                     collectArea=None, explanationGetFlg=True,
                     statsNameList=False, startPosition=1, limit=100_000,
                     updatedDate=None, callback=None):

        if self.format == "jsonp" and callback is None:
            raise(ValueError("callback option is required "
                             + "when data format is JSONP."))

        d = locals()
        d.pop("self", None)
        params = { k: v for (k, v) in d.items() if v is not None }
        params["explanationGetFlg"] = "Y" if explanationGetFlg else "N"
        params["statsNameList"] = "Y" if statsNameList else "N"

        return self.__request("GET", "getStatsList", params)


    def getMetaInfo(self, statsDataId=None, explanationGetFlg=None,
                    callback=None):

        if self.format == "jsonp" and callback is None:
            raise(ValueError("callback option is required "
                             + "when data format is JSONP."))

        d = locals()
        d.pop('self', None)
        params = { k: v for (k, v) in d.items() if v is not None }
        params["explanationGetFlg"] = "Y" if explanationGetFlg else "N"
        return self.__request("GET", "getMetaInfo", params)


    def getStatsData(self, dataSetId=None, statsDataId=None,
                     startPosition=1, limit=100_000,
                     metaGetFlg=True, cntGetFlg=False, explanationGetFlg=True,
                     annotationGetFlg=True, callback=None, sectionHeaderFlg=1, **kwargs):

        if self.format == "jsonp" and callback is None:
            raise(ValueError("callback option is required "
                             + "when data format is JSONP."))

        if self.format != "csv":
            sectionHeaderFlg=None

        d = locals()
        d.pop("self", None)
        params = { k: v for (k, v) in d.items() if v is not None }
        params["metaGetFlg"] = "Y" if metaGetFlg else "N"
        params["cntGetFlg"] = "Y" if cntGetFlg else "N"
        params["explanationGetFlg"] = "Y" if explanationGetFlg else "N"
        params["annotationGetFlg"] = "Y" if annotationGetFlg else "N"
        return self.__request("GET", "getStatsData", params)


    def postDataset(self):
        raise(NotImplementedError)

    def refDataset(self):
        pass

    def getDataCatalog(self):
        pass

    def getStatsDatas(self):
        pass
