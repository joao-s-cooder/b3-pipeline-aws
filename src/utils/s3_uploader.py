import boto3
from botocore.exceptions import ClientError
import logging
import os

class S3UpLoader:
    """
    Classe para upload de arquivos para o serviço Amazon S3.
    """
    def __init__(self, bucket_name: str, region_name: str = None):
        if not bucket_name:
            raise ValueError("O bucket_name não pode ser vazio.")
        
        self.bucket_name = bucket_name
        self.client = boto3.client('s3', region_name=region_name)
        logging.info(f"S3UpLoader inicializado com o bucket: {self.bucket_name}")

    def upload_file(self, file_path: str, object_name: str = None) -> bool:
        """
        Faz o upload de um arquivo para o bucket S3.
        
        :param file_path: Caminho do arquivo local a ser enviado.
        :param object_name: Nome do objeto no S3. Se não for especificado, será usado o nome do arquivo.
        :return: True se o upload for bem-sucedido, False caso contrário.
        """
        if not os.path.isfile(file_path):
            logging.error(f"Arquivo não encontrado: {file_path}")
            return False
        
        if object_name is None:
            object_name = os.path.basename(file_path)

        try:
            logging.info(f"Iniciando upload do arquivo '{file_path}' para o bucket '{self.bucket_name}' como '{object_name}'")
            self.client.upload_file(file_path, self.bucket_name, object_name)
            logging.info(f"Arquivo '{file_path}' enviado para '{self.bucket_name}/{object_name}' com sucesso.")
            return True
        except ClientError as e:
            logging.error(f"Erro ao enviar arquivo para S3: {e}")
            return False
        except Exception as e:
            logging.error(f"Erro inesperado ao enviar arquivo para S3: {e}")
            return False