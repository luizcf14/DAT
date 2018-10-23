import wget
import zipfile
import shapefile
from json import dumps

url = 'http://terrabrasilis.info/files/deterb/deter_public.zip'
filename = wget.download(url)
zip_ref = zipfile.ZipFile(filename, 'r')
zip_ref.extractall('.')
zip_ref.close()

filename = 'deter_public.shp'
# Ler arquivo SHP e filtrar pela classe ['DESMATAMENTO_CR','DESMATAMENTO_VEG','MINERACAO']
# Inserir em banco de dados Postgis

#Dados de Acesso (Postgis)
#usuario: solved
#senha: qazx74123
#url: pgsql03-farm62.kinghost.net:5432