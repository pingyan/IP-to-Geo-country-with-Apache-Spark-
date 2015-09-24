# A Maxmind geodb lookup function in PySpark
# @pingyan 08/2014

import GeoIP
from pyspark import SparkFiles
def geoCountry(sc, config, IPfile):
		
		# set up SparkContext with Maxmind dependency
		# config defines the path to the Mexmind dependency files
        	sc.addPyFile(config.get('geoCountry','maxmindLib'))
		sc.addFile(config.get('geoCountry','maxmindDB'))
		gi = GeoIP.open(SparkFiles.get("GeoIP.dat"),GeoIP.GEOIP_STANDARD)

	        IPfile = sc.textFile(IPfile)

                ips = IPfile.filter(lambda x: x != '') # filter by non-empty attribute

                # convert remoteAddr into geo country code
                # tmp = [(i[0],gi.country_code_by_addr(i[1])) for i in ips.collect()]
                
		geoRDD = ips.map(lambda x: gi.country_code_by_addr(x))

		return geoRDD

