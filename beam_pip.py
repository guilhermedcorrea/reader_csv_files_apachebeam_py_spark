from base64 import encode
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
import re
from apache_beam.coders.coders import Coder
from functools import wraps
import zipfile
import os


os.environ['SPARK_HOME'] = 'C:\\Users\\Guilherme\\Downloads\\spark-3.2.1-bin-hadoop3.2'

import findspark
findspark.init()

from pyspark.sql import SparkSession

arquivos_pipelines = 'D:\\testeapachebeam\\excel_files\\'


def get_file_path(path):
    """Checa se existe arquivos .csv e retorna o path"""
    search_file = re.compile('\w*\csv')
    items = search_file.search(path)
    return items


#Metodo ainda nao implantado
def search_item():
    file_name = os.listdir(arquivos_pipelines)
    excel = get_file_path(file_name)
    file = list(filter(lambda k: excel if '.csv' in k else False))
    return file

#Decorador recebe e retorna
#Metodo ainda nao implantado
def reader_excel_file(f):
    @wraps(f)
    def wrapper(*args, **kwds):
        """Calling decorated function"""

        return f(*args, **kwds)
    return wrapper

#Metodo ainda nao implantado
@reader_excel_file
def reader_excel_file_(*args, **kwargs):
    """Reader excel File"""
    
    spark = SparkSession.builder \
        .master('local[*]') \
        .appName("Iniciando com Spark") \
        .config('spark.ui.port', '4050') \
        .getOrCreate()

    df = spark.read.csv(args[0])
    return df


class CustomCoder(Coder):
    """Codificador UTF-8 Reader Files excel"""

    def encode(self, value):
        return value.encode("utf-8", "replace")

    def decode(self, value):
        return value.decode("utf-8", "ignore")

    def is_deterministic(self):
        return True


pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)


csv_file_precos = 'D:\\testeapachebeam\\excel_files\\precosgoogle0708.csv'
csv_file_marcas = 'D:\\testeapachebeam\\excel_files\\brands.csv'
csv_file_produtos = 'D:\\testeapachebeam\\excel_files\\produtobasico.csv'


#Metodo ainda nao implantado
def agrupa_marcas(*args, **kwargs):
    """
    Retorna tupla
    """
    marca, cod_bars = args
    for registro in cod_bars:
        #Verifica se existe marca ou ean
       
        if isinstance(marca, str):

            yield (f"{marca} - {registro['ean']}")
        else:
            yield (f"{marca} - {registro['ean']}")

PrecosMercado = (
    pipeline
    | "Leitura Arquivo Precos" >>
        ReadFromText(csv_file_precos, skip_header_lines=True,coder=CustomCoder())
    |"Parser csv file" >> beam.Map(lambda x: x.split(";"))
    |  "Exibe Saida" >> beam.Map(print)
    )


Marcas = (
    pipeline
    |"Leitura Arquivo Marcas" >>
        ReadFromText(csv_file_marcas, skip_header_lines=True,coder=CustomCoder())
    |"Parser csv file" >> beam.Map(lambda x: x.split(";"))
    |  "Exibe Saida" >> beam.Map(print)
    )


Produtos = (
    pipeline
    |"Leitura Arquivo Produtos" >>
        ReadFromText(csv_file_produtos, skip_header_lines=True,coder=CustomCoder())
    |"Parser csv file" >> beam.Map(lambda x: x.split(";"))
    |  "Exibe Saida" >> beam.Map(print)
    )



pipeline.run()