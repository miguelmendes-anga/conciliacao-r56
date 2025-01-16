from urllib.parse import unquote
import logging, json, boto3 
from src.helpers import *


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logging.getLogger("botocore").setLevel(logging.WARNING)


def lambda_handler(event, context):
    logger.info(event)

    try:
        message = event['Records'][0]['Sns']['Message']
        message_with_double_quotes = message.replace("'", '"')
        message_data = json.loads(message_with_double_quotes)
        
        if 'Records' not in message_data:
            return

        s3_record = message_data['Records'][0]['s3']
        s3_file_name = s3_record['object']['key']
        s3_bucket_name = s3_record['bucket']['name']
        s3_file_name = unquote(s3_file_name).replace("+", " ")

        if 'r56/' not in s3_file_name:
            return

        df = r56_reader(s3_bucket_name, s3_file_name)
    
        df = clean_and_format_data(df, s3_file_name)

        logger.info(f'Salvando parquet')
        wr.s3.to_parquet(df, f's3://anga-datalake-silver/r56/', dataset=True, mode='append', partition_cols=['year', 'month', 'day', 'origem'], database='db_anga', table='tbdwr_arquivo_r56')

        logger.info('Processo finalizado com sucesso.')

    except Exception as e:
        logger.exception(e)
        client = boto3.client("sns")
        message = f'Falha no processamento da lambda conciliacao-r56. Detalhes do erro: {e}.'
        client.publish(TargetArn='arn:aws:sns:sa-east-1:213116324309:LambdaFailsNotification', Message=message, Subject=f"Alerta. Erro de processamento: CantuService.")


if __name__=="__main__":
    event = {"Records": [
            {
                "EventSource": "aws:sns",
                "EventVersion": "1.0",
                "EventSubscriptionArn": "arn:aws:sns:sa-east-1:213116324309:AutoX-JobTabSacados:7e7e232f-134c-4d85-aa2e-57651a16648d",
                "Sns": {
                    "Type": "Notification",
                    "MessageId": "a6c7c5d8-febb-5d63-afb1-9bd689408145",
                    "TopicArn": "arn:aws:sns:sa-east-1:213116324309:AutoX-JobTabSacados",
                    "Subject": "Amazon S3 Notification",
                    "Message": "{'Records':[{'eventVersion':'2.1','eventSource':'aws:s3','awsRegion':'sa-east-1','eventTime':'2024-01-02T20: 44: 42.342Z','eventName':'ObjectCreated:Put','userIdentity':{'principalId':'AWS:AIDATDHVZBHKUXIVMO6GT'},'requestParameters':{'sourceIPAddress':'179.111.200.179'},'responseElements':{'x-amz-request-id':'T7WACQQ36GHVWW6X','x-amz-id-2':'lS+XeP+lDIfCn3xqtWqMTTugwTklVrM/bR6wAFpKaZ6DchR62rLBXoJptggnxv9VjdkSZJZcc/cwrdUdkPweechiq+BQvvAu'},'s3':{'s3SchemaVersion':'1.0','configurationId':'AutoX-RunTabSacados','bucket':{'name':'anga-datalake-bronze','ownerIdentity':{'principalId':'A3D5NJHHCIRCE2'},'arn':'arn:aws:s3: : :anga-datalake-bronze'},'object':{'key':'r56/UY3/R56_030125','size':249,'eTag':'07116e579c44288cb4d17b626b127dca','versionId':'r0AnxsqlrkZ4zHFQ4FSAmiO1sd0drghQ','sequencer':'00659475BA4A93FBC7'}}}]}",
                    "Timestamp": "2024-01-02T20:44:43.525Z",
                    "SignatureVersion": "1",
                    "Signature": "bdSUSx8CgHlHKTGbHX1i5qxlKwDA47csmuzsuU8oCuCubvozzmxVTJW/2B+MoKVsa7+WFXvj4xiLyrYlRT0iedrPPz0o6CbYncbcaC2G1vF0j7r9OdKDMuM2LPEVMGR63/3KNvRgud4ykkimHClt7i9E5P2441+RSOKbDmunCNAZUUlpk1yIInBHh3dji+ZwO66FVLlbSXjc1m8bQzhl8piimkv0cH271pVKUZSCyrDpNhU9S4/4k0ZT6Gxifnl7l69jMsCwMDMurdP4pWz00MysQkUa/3ef+no1dcrNJERv0E8WRud/3P9mod4ewmMW9G9VInXL4RZOt6BShtpwZA==",
                    "SigningCertUrl": "https://sns.sa-east-1.amazonaws.com/SimpleNotificationService-01d088a6f77103d0fe307c0069e40ed6.pem",
                    "UnsubscribeUrl": "https://sns.sa-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:sa-east-1:213116324309:AutoX-JobTabSacados:7e7e232f-134c-4d85-aa2e-57651a16648d",
                    "MessageAttributes": {}
                }
            }
        ]
    }

    lambda_handler(event, None)