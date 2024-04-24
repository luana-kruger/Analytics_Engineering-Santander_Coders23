import os
import gzip
import shutil
import requests
from concurrent.futures import ThreadPoolExecutor

class Landing:
    """
    A classe `Landing` gerencia o download e processamento de dados da etapa de landing em um projeto de engenharia de dados.

    Atributos:
        landing_path (str): O caminho para o diretório onde os dados baixados serão salvos.

    Responsabilidades:
        - Baixar arquivos de URLs fornecidas de forma concorrente.
        - Salvar os arquivos baixados no diretório de destino.
        - Descompactar arquivos gzip no diretório de destino (opcional).

    Exceções:
        - FileNotFoundError: Se um arquivo a ser descompactado não for encontrado.
        - Exception: Se ocorrer um erro inesperado durante a criação do diretório de destino ou o download/descompressão de arquivos.
    """
    def __init__(self,landing_path:str) -> None:
        """
        Inicializa a classe Landing.

        Args:
            landing_path (str): O caminho para o diretório onde os arquivos baixados serão salvos.
                Se o diretório não existir, ele será criado.

        Raises:
            Exception: Se ocorrer um erro inesperado ao criar o diretório de destino.
        """
        if ~os.path.exists(landing_path):
            try:
                print(f"O diretório {landing_path} não existe e tentaremos criar.")
                os.makedirs(landing_path)
                print(f"Diretório criado com sucesso!!")
            except FileExistsError:
                print(f"O diretório {landing_path} já existe e nada será feito!!")
            except Exception as e:
                print(f"Erro inesperado ao criar o diretório {landing_path}: {e}")
        self.landing_path = landing_path

    def extract(self,url:list[str]):
        """
        Baixa arquivos das URLs fornecidas de forma concorrente e os salva no diretório de destino.

        Args:
            urls (list[str]): Uma lista de URLs de onde baixar os arquivos.
        """

        def get_data_and_download(url:str):
            """
            Baixa um arquivo da URL especificada e o salva no diretório de destino.

            Args:
                url (str): A URL do arquivo para download.

            Raises:
                requests.RequestException: Se ocorrer um erro durante a solicitação HTTP.
                Exception: Se ocorrer um erro inesperado.
            """

            try:
                print(f"Efetuando a requisição para a url {url}")
                response = requests.get(url=url,stream=True)
                response.raise_for_status()

                save_path = os.path.join(self.landing_path,url.split('/')[-1])
                print(f"Salvando o arquivo {save_path}")
                with open(save_path,'wb') as response_file:
                    response_file.write(response.content)
            except requests.RequestException as req:
                print(f"Erro ao efetuar a requisição na url {url}: {req}")
            except Exception as e:
                print(f"Erro inesperado ao efetuar a requisição na url {url}: {req}")

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(get_data_and_download,link) for link in url]
        
        for future in futures:
            future.result()

    def transform(self):
        """
        Descompacta arquivos gzip no diretório de destino, substituindo os arquivos originais.

        Raises:
            FileNotFoundError: Se um arquivo a ser descompactado não for encontrado.
        """
        landing_path = self.landing_path
        list_of_files = os.listdir(landing_path)

        # Extract files
        for file in list_of_files:
            file = os.path.join(landing_path,file)
            with gzip.open(file,'rb') as f_in:
                with open(file[:-3],'wb') as f_out:
                    shutil.copyfileobj(f_in,f_out)

        # Remove files
        for file in list_of_files:
            file = os.path.join(landing_path,file)
            os.remove(file)