import os
import zipfile
from TransformacaoRomming import TrasformarRomming


class PacoteFuncoes():

        @staticmethod
        def descompactar(dir_name, extension):
        # change directory from working dir to dir with files
                os.chdir(dir_name)

                for item in os.listdir(dir_name):  # loop through items in dir
                        if item.endswith(extension):  # check for ".zip" extension
                                file_name = os.path.abspath(item)  # get full path of files
                                # create zipfile object
                                zip_ref = zipfile.ZipFile(file_name)
                                zip_ref.extractall(dir_name)  # extract file to dir
                                zip_ref.close()  # close file
                                os.remove(file_name) # delete zipped

        @staticmethod
        def converter(extension, dir_name):
            os.chdir(dir_name)  # muda para o diretorio ques esta os arquivos
            # olha os arquivos que estao no diretorio
            for item in os.listdir(dir_name):
                # procura os arquivos q terminam com a extencao passada
                if item.endswith(extension) and extension=='.GS3':
                # pega o caminho td do arquivo
                        file_name = os.path.abspath(item)
                        TrasformarRomming.transformarGS3(file_name, item)
                        os.remove(file_name)

