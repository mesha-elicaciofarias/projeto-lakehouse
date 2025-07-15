import boto3
from io import BytesIO

# Variáveis de ambiente (ajuste conforme seu .env)
minio_endpoint = "http://localhost:10000"  # se estiver fora do Docker
access_key = "SEU_ACCESS_KEY"
secret_key = "SUA_SECRET_KEY"
bucket = "landing"

# Cliente MinIO/S3
s3 = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:10000",
    aws_access_key_id="hmU6JbqMhAauZ8qacHji",
    aws_secret_access_key="W94W0C4ih2onju3jvtBykw1uLpyZnEgK9JJPu3RL",
)

# Teste de upload
conteudo = b"Testando upload no MinIO via boto3"
arquivo = BytesIO(conteudo)

s3.upload_fileobj(arquivo, bucket, "teste/minio_arquivo.txt")
print("Upload realizado com sucesso!")
